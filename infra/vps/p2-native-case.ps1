param(
    [Parameter(Mandatory=$true)][string]$CaseDirectory,
    [Parameter(Mandatory=$true)][string]$K6Path,
    [string]$BaseUrl='http://127.0.0.1:43083'
)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$repo=(Resolve-Path "$PSScriptRoot/../..").Path
New-Item -ItemType Directory -Path $CaseDirectory -Force | Out-Null
$memory=Get-CimInstance Win32_OperatingSystem
if ($memory.FreePhysicalMemory -lt 1536*1024 -or $memory.FreeVirtualMemory -lt 1024*1024) {
    throw 'Insufficient Windows memory headroom'
}
if (Get-NetTCPConnection -LocalPort 6566 -State Listen -ErrorAction SilentlyContinue) {
    throw 'Native k6 status port already owned'
}
if ((Invoke-RestMethod -TimeoutSec 10 "$BaseUrl/ready").status -ne 'ready') {
    throw 'Native Windows common entry is not ready'
}
$process=$null
$watchdog=$null
$state=@{passed=$false;exit_code=$null;error=$null}
try {
    foreach ($line in Get-Content -LiteralPath "$repo/.env") {
        if ($line -match '^AGGREGATOR_API_KEY=(.*)$') {
            $env:AGGREGATOR_API_KEY=$Matches[1].Trim().Trim('"').Trim("'")
        }
    }
    if (-not $env:AGGREGATOR_API_KEY) { throw 'Aggregator credential missing' }
    $env:AGGREGATOR_BASE_URL=$BaseUrl
    $env:VUS='1000'; $env:DURATION='60s'; $env:GOMAXPROCS='6'; $env:K6_NO_USAGE_REPORT='true'
    $arguments=@('run','--quiet','--address=127.0.0.1:6566','--summary-export',
                 ('"'+$CaseDirectory+'/summary.json"'),('"'+$repo+'/scripts/m2/p2/throughput.js"'))
    $process=Start-Process -WindowStyle Hidden -PassThru -FilePath $K6Path -ArgumentList $arguments `
        -RedirectStandardOutput "$CaseDirectory/stdout.log" -RedirectStandardError "$CaseDirectory/stderr.log"
    $null=$process.Handle
    $watchArgs=@('-NoProfile','-ExecutionPolicy','Bypass','-File',('"'+$PSScriptRoot+'/watch-load.ps1"'),
                 '-GeneratorPid',$process.Id,'-StartTicks',$process.StartTime.ToUniversalTime().Ticks,
                 '-RunDir',('"'+$CaseDirectory+'"'),'-MaxPrivateMiB',1536)
    $watchdog=Start-Process -WindowStyle Hidden -PassThru -FilePath powershell.exe -ArgumentList $watchArgs `
        -RedirectStandardOutput "$CaseDirectory/watchdog-stdout.log" -RedirectStandardError "$CaseDirectory/watchdog-stderr.log"
    $clock=[Diagnostics.Stopwatch]::StartNew()
    $execution=$null
    while (-not $process.HasExited) {
        if ($clock.Elapsed.TotalSeconds -gt 100) { throw 'Native generator exceeded overall timeout' }
        $resource=Get-Item -LiteralPath "$CaseDirectory/generator-resources.json" -ErrorAction SilentlyContinue
        if ($clock.Elapsed.TotalSeconds -gt 15 -and (-not $resource -or ([DateTime]::UtcNow-$resource.LastWriteTimeUtc).TotalSeconds -gt 10)) {
            throw 'Independent native memory monitor missing or stale'
        }
        try {
            $status=(Invoke-RestMethod -TimeoutSec 1 http://127.0.0.1:6566/v1/status).data.attributes
            if ($status.running) {
                if ($null -eq $execution) { $execution=$clock.Elapsed.TotalSeconds }
                @{type='status';elapsed=$clock.Elapsed.TotalSeconds-$execution;vus=$status.vus;running=$status.running} | ConvertTo-Json -Compress
            }
        } catch { }
        Start-Sleep -Milliseconds 750
        $process.Refresh()
    }
    $process.WaitForExit()
    if (-not $watchdog.WaitForExit(10000)) { throw 'Native watchdog did not finish' }
    $resources=Get-Content -Raw -LiteralPath "$CaseDirectory/generator-resources.json" | ConvertFrom-Json
    $state.exit_code=$process.ExitCode
    if ($resources.safety_stop_reason) { throw $resources.safety_stop_reason }
    if ($process.ExitCode -ne 0) { throw "Native k6 exit $($process.ExitCode)" }
    $state.passed=$true
} catch {
    $state.error=$_.Exception.Message
} finally {
    if ($process) {
        $process.Refresh()
        if (-not $process.HasExited) { Stop-Process -Id $process.Id; $process.WaitForExit() }
    }
    if ($watchdog -and -not $watchdog.HasExited) { $null=$watchdog.WaitForExit(10000) }
    $state | ConvertTo-Json | Set-Content -Encoding UTF8 "$CaseDirectory/native-result.json"
    Remove-Item Env:AGGREGATOR_API_KEY -ErrorAction SilentlyContinue
}
@{type='final';passed=$state.passed;error=$state.error} | ConvertTo-Json -Compress
if (-not $state.passed) { exit 1 }
