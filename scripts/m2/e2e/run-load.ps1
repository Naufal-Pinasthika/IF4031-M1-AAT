param(
    [ValidateSet('priority','rolling','throughput','scale')][string]$Case='priority',
    [string]$K6Path='D:\aat-m2-e2e\k6\k6-v2.4.0-windows-amd64\k6.exe',
    [string]$Distro='AAT-M2-E2E',
    [string]$UpgradeImage='',
    [string]$RollingDuration='120s',
    [ValidateRange(1,10000)][int]$LoadVus=10000,
    [ValidateRange(128,16384)][int]$MaxGeneratorMemoryMiB=4096
)
$ErrorActionPreference='Stop'
$repo=(Resolve-Path "$PSScriptRoot/../../..").Path
$lab='/mnt/'+$repo.Substring(0,1).ToLower()+$repo.Substring(2).Replace('\','/')+'/scripts/m2/e2e/lab.sh'
$namespace='sistem-informasi-kebencanaan-bnpb'
foreach ($line in Get-Content -LiteralPath "$repo/.env") {
    if ($line -match '^([A-Z][A-Z0-9_]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($Matches[1],$Matches[2].Trim().Trim('"').Trim("'"),'Process')
    }
}
$env:AGGREGATOR_BASE_URL='http://127.0.0.1:30083'
$env:AUTH_BASE_URL='http://127.0.0.1:30084'
$env:CLIENT_API_BASE_URL='http://127.0.0.1:30080'
function Kube {
    & wsl.exe -d $Distro --exec bash $lab kube --request-timeout=10s -n $namespace @args
    if ($LASTEXITCODE -ne 0) { throw 'Kubernetes command failed' }
}
function Start-Load([string]$name,[string]$script) {
    if ((Get-PSDrive C).Free -lt 1GB) { throw 'Less than 1 GiB free on C:; refusing load generation' }
    $process=Start-Process -FilePath $K6Path -WindowStyle Hidden -PassThru -ArgumentList @(
        'run','--quiet','--summary-export',"$repo/.e2e/$name-summary.json","$repo/$script"
    ) -RedirectStandardOutput "$repo/.e2e/$name-stdout.log" -RedirectStandardError "$repo/.e2e/$name-stderr.log"
    $null=$process.Handle
    $process | Add-Member -NotePropertyName EvidenceName -NotePropertyValue $name
    return $process
}
function Wait-Load($process, [scriptblock]$during=$null) {
    $watch=[Diagnostics.Stopwatch]::StartNew()
    $peakPrivate=0L; $peakResident=0L; $reason=$null; $lowestLinuxAvailableKiB=[long]::MaxValue
    try {
      while (-not $process.HasExited) {
        $peakPrivate=[math]::Max($peakPrivate,$process.PrivateMemorySize64)
        $peakResident=[math]::Max($peakResident,$process.PeakWorkingSet64)
        if ((Get-PSDrive C).Free -lt 1GB) {
            $reason='Less than 1 GiB free on C:'
        }
        if ($process.PrivateMemorySize64 -gt $MaxGeneratorMemoryMiB*1MB) { $reason='Generator exceeded its explicit memory budget' }
        $taskHost=Get-CimInstance Win32_OperatingSystem
        if ($taskHost.FreeVirtualMemory -lt 512*1024) { $reason='Host commit headroom fell below 512 MiB' }
        # The relay and VM host share WSL's separate memory ceiling. Windows
        # commit headroom alone cannot detect Linux-side OOM pressure.
        $taskLinuxMemory=& wsl.exe -d $Distro --exec cat /proc/meminfo
        if ($LASTEXITCODE -ne 0) { $reason='Could not inspect Linux memory safety headroom' }
        foreach ($taskMemoryLine in $taskLinuxMemory) {
            if ($taskMemoryLine -match '^MemAvailable:\s+(\d+)\s+kB') {
                $availableKiB=[long]$Matches[1]
                $lowestLinuxAvailableKiB=[math]::Min($lowestLinuxAvailableKiB,$availableKiB)
                if ($availableKiB -lt 256*1024) { $reason='Linux memory headroom fell below 256 MiB' }
            }
        }
        if ($reason) { Stop-Process -Id $process.Id; break }
        if ($during) { & $during $watch.Elapsed.TotalSeconds }
        Start-Sleep -Seconds 2
        $process.Refresh()
      }
    } catch {
        $reason=$_.Exception.Message
        throw
    } finally {
      $process.Refresh()
      if (-not $process.HasExited) {
        $peakPrivate=[math]::Max($peakPrivate,$process.PrivateMemorySize64)
        $peakResident=[math]::Max($peakResident,$process.PeakWorkingSet64)
      }
      if (-not $process.HasExited) { Stop-Process -Id $process.Id }
      $process.WaitForExit()
      @{elapsed_seconds=$watch.Elapsed.TotalSeconds;peak_private_mib=[math]::Round($peakPrivate/1MB,2);peak_resident_mib=[math]::Round($peakResident/1MB,2);lowest_linux_available_mib=[math]::Round($lowestLinuxAvailableKiB/1024,2);safety_stop_reason=$reason;exit_code=$process.ExitCode} |
        ConvertTo-Json | Set-Content "$repo/.e2e/$($process.EvidenceName)-generator-resources.json"
    }
    if ($reason) { throw "Stopped only this load generator: $reason" }
    if ($process.ExitCode -ne 0) { throw "k6 thresholds failed (exit $($process.ExitCode)); inspect .e2e evidence" }
}
if ($Case -eq 'priority') {
    $env:MEDIA_VUS='50'; $env:DURATION='45s'
    Wait-Load (Start-Load 'priority' 'scripts/m2/p5/05-priority-load.js')
} elseif ($Case -eq 'rolling') {
    $env:VUS='10'; $env:DURATION=$RollingDuration
    $load=Start-Load 'rolling' 'scripts/m2/p2/rolling.js'
    Start-Sleep -Seconds 10
    if ($load.HasExited) { throw 'Load ended before the rollout began.' }
    if ($UpgradeImage) {
        Kube set image deployment/aggregator "aggregator=$UpgradeImage"
    } else {
        Kube rollout restart deployment/aggregator
    }
    Kube rollout status deployment/aggregator --timeout=180s
    $load.Refresh()
    if ($load.HasExited) { throw 'Load ended before the complete rollout was observed; this run cannot prove zero-failure rollout.' }
    Wait-Load $load
} elseif ($Case -eq 'scale') {
    $env:VUS=[string]$LoadVus; $env:HOLD_DURATION='120s'
    $taskScale=@{up=$false;down=$false;saw_five=$false;saw_two=$false;snapshots=@();last_sample=-10;passed=$false;vus=$LoadVus;hold_seconds=120}
    $load=$null
    try {
        Kube scale deployment/aggregator --replicas=3
        Kube rollout status deployment/aggregator --timeout=180s
        $load=Start-Load "scale-$LoadVus" 'scripts/m2/p2/load.js'
        Wait-Load $load {
            param($seconds)
            if ($seconds -ge 60 -and -not $taskScale.up) { Kube scale deployment/aggregator --replicas=5; $taskScale.up=$true }
            if ($seconds -ge 110 -and -not $taskScale.down) { Kube scale deployment/aggregator --replicas=2; $taskScale.down=$true }
            if ($seconds-$taskScale.last_sample -ge 5) {
                $ready=[int](Kube get deployment aggregator '-o=jsonpath={.status.readyReplicas}')
                $taskScale.snapshots+=@{seconds=[math]::Round($seconds,2);ready=$ready}
                if ($taskScale.up -and -not $taskScale.down -and $ready -eq 5) { $taskScale.saw_five=$true }
                if ($taskScale.down -and $ready -eq 2) { $taskScale.saw_two=$true }
                $taskScale.last_sample=$seconds
            }
        }
        if (-not $taskScale.saw_five -or -not $taskScale.saw_two) { throw 'Requested replica counts were not both observed Ready during the test.' }
        $taskScale.passed=$true
    } finally {
        if ($load -and -not $load.HasExited) { Stop-Process -Id $load.Id }
        $taskScale | ConvertTo-Json -Depth 5 | Set-Content "$repo/.e2e/scale-$LoadVus-results.json"
        Kube scale deployment/aggregator --replicas=3
    }
} else {
    $env:VUS='50'; $env:DURATION='30s'
    try {
        foreach ($replicas in @(1,3)) {
            Kube scale deployment/aggregator "--replicas=$replicas"
            Kube rollout status deployment/aggregator --timeout=180s
            Start-Sleep -Seconds 10
            Wait-Load (Start-Load "throughput-$replicas" 'scripts/m2/p2/throughput.js')
        }
        $one=Get-Content -Raw "$repo/.e2e/throughput-1-summary.json" | ConvertFrom-Json
        $three=Get-Content -Raw "$repo/.e2e/throughput-3-summary.json" | ConvertFrom-Json
        $oneRate=$one.metrics.successful_requests.rate
        $threeRate=$three.metrics.successful_requests.rate
        $ratio=$threeRate/$oneRate
        @{one_success_rps=$oneRate;three_success_rps=$threeRate;ratio=$ratio;passed=($ratio -ge 2);vus=50;duration_seconds=30} |
            ConvertTo-Json | Set-Content "$repo/.e2e/throughput-results.json"
        if ($ratio -lt 2) { throw "Successful throughput ratio $ratio is below 2.0" }
    } finally {
        Kube scale deployment/aggregator --replicas=3
    }
}
