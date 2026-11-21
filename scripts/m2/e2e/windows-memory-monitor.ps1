$ErrorActionPreference='Stop'
# One short-lived helper per run. Native counters avoid repeated WMI startup
# while the host is paging. A newline on stdin requests a graceful stop.
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
public static class LabMemory {
    [StructLayout(LayoutKind.Sequential)]
    public struct Status {
        public uint Length, Load;
        public ulong TotalPhys, AvailablePhys, TotalPageFile, AvailablePageFile;
        public ulong TotalVirtual, AvailableVirtual, AvailableExtendedVirtual;
    }
    [DllImport("kernel32.dll", SetLastError=true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool GlobalMemoryStatusEx(ref Status status);
    public static Task<string> ReadStopAsync() {
        return Task.Run(() => Console.ReadLine());
    }
}
'@
$stopRead=[LabMemory]::ReadStopAsync()
$taskDeadline=[DateTime]::UtcNow.AddMinutes(10)
while (-not $stopRead.IsCompleted -and [DateTime]::UtcNow -lt $taskDeadline) {
    $memory=New-Object LabMemory+Status
    $memory.Length=[Runtime.InteropServices.Marshal]::SizeOf($memory)
    if (-not [LabMemory]::GlobalMemoryStatusEx([ref]$memory)) { throw 'GlobalMemoryStatusEx failed' }
    [Console]::WriteLine([string]([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())+','+[string]([long]($memory.AvailablePageFile/1KB))+','+[string]([long]($memory.AvailablePhys/1KB)))
    [Console]::Out.Flush()
    Start-Sleep -Milliseconds 500
}
