param(
    [ValidateRange(1,10000)][int]$Vus=10000,
    [ValidateRange(120,180)][int]$HoldSeconds=130,
    [string]$K6Path='D:\aat-m2-e2e\k6\k6-v2.4.0-windows-amd64\k6.exe'
)
$ErrorActionPreference='Stop'
$repo=(Resolve-Path "$PSScriptRoot/../..").Path
$linuxRepo='/mnt/'+$repo.Substring(0,1).ToLower()+$repo.Substring(2).Replace('\','/')
$namespace='sistem-informasi-kebencanaan-bnpb'
$run=Join-Path $repo ('.vps/load-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')+'-'+$Vus)
New-Item -ItemType Directory -Path $run | Out-Null
$state=@{passed=$false;vus=$Vus;hold_seconds=$HoldSeconds;started_at=[DateTime]::UtcNow.ToString('o');transport='SSH tunnel';snapshots=@();vu_samples=@();saw_five=$false;saw_two=$false;error=$null}
function Kube {
    $output=& wsl.exe -d Ubuntu-22.04 --exec python3 "$linuxRepo/infra/vps/access.py" kube -- --request-timeout=10s -n $namespace @args
    if ($LASTEXITCODE -ne 0) { throw "VPS Kubernetes command failed (exit $LASTEXITCODE): $args; $output" }
    return ($output -join "`n")
}
$load=$null; $watchdog=$null; $watch=$null; $executionStart=$null
try {
    $memory=Get-CimInstance Win32_OperatingSystem
    if ($memory.FreePhysicalMemory -lt 5*1024*1024 -or $memory.FreeVirtualMemory -lt 6*1024*1024) { throw 'Need at least 5 GiB physical and 6 GiB commit headroom before this 10k run' }
    if ((Get-PSDrive C).Free -lt 1GB) { throw 'Insufficient disk headroom' }
    if ((Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 http://127.0.0.1:30083/ready).StatusCode -ne 200) { throw 'VPS tunnel is not ready' }
    Kube scale deployment/aggregator --replicas=3 | Out-Null
    Kube rollout status deployment/aggregator --timeout=180s | Out-Null
    Kube get pods -l app=aggregator -o json | Set-Content -Encoding UTF8 "$run/before-pods.json"
    foreach ($line in Get-Content -LiteralPath "$repo/.env") {
        if ($line -match '^AGGREGATOR_API_KEY=(.*)$') { $env:AGGREGATOR_API_KEY=$Matches[1].Trim().Trim('"').Trim("'") }
    }
    if (-not $env:AGGREGATOR_API_KEY) { throw 'Aggregator key is missing' }
    $env:AGGREGATOR_BASE_URL='http://127.0.0.1:30083'
    $env:VUS=[string]$Vus; $env:HOLD_DURATION=[string]$HoldSeconds+'s'
    $env:GOMAXPROCS='6'; $env:K6_NO_USAGE_REPORT='true'
    $load=Start-Process -WindowStyle Hidden -PassThru -FilePath $K6Path -ArgumentList @('run','--quiet','--address=127.0.0.1:6566','--summary-export',"$run/summary.json","$repo/scripts/m2/p2/load.js") -RedirectStandardOutput "$run/stdout.log" -RedirectStandardError "$run/stderr.log"
    $null=$load.Handle
    $watchdog=Start-Process -WindowStyle Hidden -PassThru -FilePath powershell.exe -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',"$PSScriptRoot/watch-load.ps1",'-GeneratorPid',$load.Id,'-StartTicks',$load.StartTime.ToUniversalTime().Ticks,'-RunDir',$run) -RedirectStandardOutput "$run/watchdog-stdout.log" -RedirectStandardError "$run/watchdog-stderr.log"
    $watch=[Diagnostics.Stopwatch]::StartNew()
    $up=$false; $down=$false; $lastSnapshot=-5; $lastApi=-10
    while (-not $load.HasExited) {
        if ($watch.Elapsed.TotalSeconds -gt 330) { throw 'Generator exceeded overall time budget' }
        $resourceFile=Get-Item -LiteralPath "$run/generator-resources.json" -ErrorAction SilentlyContinue
        if ($watch.Elapsed.TotalSeconds -gt 15 -and (-not $resourceFile -or ([DateTime]::UtcNow-$resourceFile.LastWriteTimeUtc).TotalSeconds -gt 10)) { throw 'Independent memory watchdog telemetry missing or stale' }
        try {
            $status=Invoke-RestMethod -TimeoutSec 1 http://127.0.0.1:6566/v1/status
            if ($status.data.attributes.running -and -not $executionStart) { $executionStart=$watch.Elapsed.TotalSeconds }
            $lastApi=$watch.Elapsed.TotalSeconds
            $elapsed=if ($null -eq $executionStart) { 0 } else { $watch.Elapsed.TotalSeconds-$executionStart }
            $state.vu_samples+=@{seconds=[math]::Round($elapsed,2);vus=$status.data.attributes.vus;running=$status.data.attributes.running}
        } catch {
            if ($executionStart -and $watch.Elapsed.TotalSeconds-$lastApi -gt 10) { throw 'k6 REST telemetry stale during execution' }
        }
        if ($null -ne $executionStart) {
            $elapsed=$watch.Elapsed.TotalSeconds-$executionStart
            if ($elapsed -ge 60 -and -not $up) { Kube scale deployment/aggregator --replicas=5 | Out-Null; $up=$true; Write-Output 'Requested 5 Aggregators during load' }
            if ($elapsed -ge 110 -and -not $down) { Kube scale deployment/aggregator --replicas=2 | Out-Null; $down=$true; Write-Output 'Requested 2 Aggregators during load' }
            if ($elapsed-$lastSnapshot -ge 5) {
                $dep=Kube get deployment aggregator -o json | ConvertFrom-Json
                $state.snapshots+=@{seconds=[math]::Round($elapsed,2);desired=$dep.spec.replicas;ready=$dep.status.readyReplicas;updated=$dep.status.updatedReplicas;available=$dep.status.availableReplicas}
                if ([int]$elapsed % 20 -lt 5) {
                    Kube get --raw=/apis/metrics.k8s.io/v1beta1/nodes | Set-Content -Encoding UTF8 "$run/nodes-$([int]$elapsed).json"
                }
                if ($up -and -not $down -and $dep.status.readyReplicas -eq 5) { $state.saw_five=$true }
                if ($down -and $dep.status.readyReplicas -eq 2) { $state.saw_two=$true }
                $lastSnapshot=$elapsed
                $state | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 "$run/results.json"
            }
        }
        Start-Sleep -Milliseconds 750
        $load.Refresh()
    }
    $load.WaitForExit(); $watchdog.WaitForExit()
    $state.exit_code=$load.ExitCode
    $resources=Get-Content -Raw "$run/generator-resources.json" | ConvertFrom-Json
    if ($resources.safety_stop_reason) { throw $resources.safety_stop_reason }
    if ($load.ExitCode -ne 0) { throw "k6 failed (exit $($load.ExitCode)); evidence retained" }
    if (-not $state.saw_five -or -not $state.saw_two) { throw 'Both scale states were not observed Ready during load' }
    $full=@($state.vu_samples | Where-Object { $_.running -and $_.vus -ge $Vus })
    $state.observed_full_vu_span_seconds=if ($full.Count -gt 1) { $full[-1].seconds-$full[0].seconds } else { 0 }
    if ($state.observed_full_vu_span_seconds -lt 120) { throw 'Less than 120 seconds of observed target VUs' }
    $state.passed=$true
} catch {
    $state.error=$_.Exception.Message
    Write-Output ('Load test did not pass: '+$state.error)
} finally {
    if ($load) {
        $load.Refresh()
        if (-not $load.HasExited) { Stop-Process -Id $load.Id; $load.WaitForExit() }
    }
    if ($watchdog -and -not $watchdog.HasExited) { $null=$watchdog.WaitForExit(10000) }
    $state.finished_at=[DateTime]::UtcNow.ToString('o')
    $state | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 "$run/results.json"
    Kube scale deployment/aggregator --replicas=3 | Out-Null
    Kube rollout status deployment/aggregator --timeout=180s | Out-Null
    Kube get pods -l app=aggregator -o json | Set-Content -Encoding UTF8 "$run/after-pods.json"
    Write-Output "Evidence: $run"
    Remove-Item Env:AGGREGATOR_API_KEY -ErrorAction SilentlyContinue
}
if (-not $state.passed) { exit 1 }
