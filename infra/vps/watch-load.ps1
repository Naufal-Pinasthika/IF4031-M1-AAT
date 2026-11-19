param(
    [Parameter(Mandatory=$true)][int]$GeneratorPid,
    [Parameter(Mandatory=$true)][long]$StartTicks,
    [Parameter(Mandatory=$true)][string]$RunDir,
    [int]$MaxPrivateMiB=5120
)
$ErrorActionPreference='Stop'
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class AatLoadMemory {
  [StructLayout(LayoutKind.Sequential)]
  public struct Status {
    public uint Length, Load;
    public ulong TotalPhys, AvailablePhys, TotalPageFile, AvailablePageFile;
    public ulong TotalVirtual, AvailableVirtual, AvailableExtendedVirtual;
  }
  [DllImport("kernel32.dll", SetLastError=true)]
  [return: MarshalAs(UnmanagedType.Bool)]
  public static extern bool GlobalMemoryStatusEx(ref Status status);
}
'@
$result=@{samples=@();safety_stop_reason=$null;peak_private_mib=0;peak_resident_mib=0;peak_cpu_cores=0}
$clock=[Diagnostics.Stopwatch]::StartNew()
$previousCpu=$null; $previousTime=0
try {
  while ($clock.Elapsed.TotalSeconds -lt 360) {
    $taskProcess=Get-Process -Id $GeneratorPid -ErrorAction SilentlyContinue
    if (-not $taskProcess) { break }
    if ($taskProcess.ProcessName -ne 'k6' -or $taskProcess.StartTime.ToUniversalTime().Ticks -ne $StartTicks) { throw 'Generator identity changed' }
    $m=New-Object AatLoadMemory+Status
    $m.Length=[Runtime.InteropServices.Marshal]::SizeOf($m)
    if (-not [AatLoadMemory]::GlobalMemoryStatusEx([ref]$m)) { throw 'Native memory telemetry failed' }
    $seconds=$clock.Elapsed.TotalSeconds
    $cpu=$taskProcess.TotalProcessorTime.TotalSeconds
    $cores=if ($null -eq $previousCpu) { 0 } else { ($cpu-$previousCpu)/[math]::Max(0.001,$seconds-$previousTime) }
    $previousCpu=$cpu; $previousTime=$seconds
    $private=$taskProcess.PrivateMemorySize64/1MB; $rss=$taskProcess.WorkingSet64/1MB
    $physical=$m.AvailablePhys/1MB; $commit=$m.AvailablePageFile/1MB
    $result.peak_private_mib=[math]::Max($private,$result.peak_private_mib)
    $result.peak_resident_mib=[math]::Max($rss,$result.peak_resident_mib)
    $result.peak_cpu_cores=[math]::Max($cores,$result.peak_cpu_cores)
    $result.samples+=@{seconds=[math]::Round($seconds,2);private_mib=[math]::Round($private,2);resident_mib=[math]::Round($rss,2);cpu_cores=[math]::Round($cores,2);available_physical_mib=[math]::Round($physical,2);available_commit_mib=[math]::Round($commit,2)}
    if ($physical -lt 768) { $result.safety_stop_reason='Windows physical headroom below 768 MiB' }
    if ($commit -lt 768) { $result.safety_stop_reason='Windows commit headroom below 768 MiB' }
    if ($private -gt $MaxPrivateMiB) { $result.safety_stop_reason='Generator private memory exceeded explicit budget' }
    $result | ConvertTo-Json -Depth 5 | Set-Content -Encoding UTF8 "$RunDir/generator-resources.json"
    if ($result.safety_stop_reason) { Stop-Process -Id $GeneratorPid; break }
    Start-Sleep -Milliseconds 750
  }
} catch {
  $result.safety_stop_reason=$_.Exception.Message
  $taskProcess=Get-Process -Id $GeneratorPid -ErrorAction SilentlyContinue
  if ($taskProcess -and $taskProcess.ProcessName -eq 'k6' -and $taskProcess.StartTime.ToUniversalTime().Ticks -eq $StartTicks) { Stop-Process -Id $GeneratorPid }
} finally {
  $result.elapsed_seconds=$clock.Elapsed.TotalSeconds
  $result | ConvertTo-Json -Depth 5 | Set-Content -Encoding UTF8 "$RunDir/generator-resources.json"
}
