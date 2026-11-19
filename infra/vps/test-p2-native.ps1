param()
$ErrorActionPreference='Stop'
$repo=(Resolve-Path "$PSScriptRoot/../..").Path
$linuxRepo='/mnt/'+$repo.Substring(0,1).ToLower()+$repo.Substring(2).Replace('\','/')
$inventory=Get-Content -Raw -LiteralPath "$PSScriptRoot/inventory.json" | ConvertFrom-Json
$node=$inventory.nodes[0]
$keySource='//wsl.localhost/Ubuntu-22.04'+$inventory.key
$knownSource='//wsl.localhost/Ubuntu-22.04'+($inventory.key.Substring(0,$inventory.key.LastIndexOf('/'))+'/known_hosts')
$temporary=Join-Path $repo ('.vps/native-ssh-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
$key=Join-Path $temporary 'identity.pem'
$ssh=$null
$testExit=1
try {
    if (Get-NetTCPConnection -LocalPort 43083 -State Listen -ErrorAction SilentlyContinue) {
        throw 'Native tunnel port already owned; refusing to replace it'
    }
    New-Item -ItemType Directory -Path $temporary | Out-Null
    $identity=[Security.Principal.WindowsIdentity]::GetCurrent().User
    $acl=New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner($identity)
    $acl.SetAccessRuleProtection($true,$false)
    $rule=New-Object Security.AccessControl.FileSystemAccessRule($identity,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
    $acl.AddAccessRule($rule)
    Set-Acl -LiteralPath $temporary -AclObject $acl
    # Opaque credential copy into an already private directory. Never print,
    # patch, log or alter the source credential. Git ignores .vps entirely.
    Copy-Item -LiteralPath $keySource -Destination $key
    $sshArgs=@('-i',$key.Replace('\','/'),'-o',('UserKnownHostsFile='+$knownSource),
               '-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=15',
               '-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=4',
               '-N','-L',('127.0.0.1:43083:'+ $node.private_ip+':30083'),($node.user+'@'+$node.host))
    $ssh=Start-Process -WindowStyle Hidden -PassThru -FilePath "$env:WINDIR/System32/OpenSSH/ssh.exe" -ArgumentList $sshArgs `
        -RedirectStandardOutput "$repo/.vps/native-ssh-stdout.log" -RedirectStandardError "$repo/.vps/native-ssh-stderr.log"
    Start-Sleep -Seconds 2
    $ssh.Refresh()
    if ($ssh.HasExited) { throw 'Native SSH failed; inspect private tunnel log' }
    & wsl.exe -d Ubuntu-22.04 --cd $linuxRepo --exec python3 infra/vps/test-p2-laptop.py --native-windows --external-native-tunnel
    $testExit=$LASTEXITCODE
} finally {
    if ($ssh) {
        $ssh.Refresh()
        if (-not $ssh.HasExited) { Stop-Process -Id $ssh.Id; $ssh.WaitForExit() }
    }
    # Exact new targets only; no recursive deletion or existing-key mutation.
    if (Test-Path -LiteralPath $key) { Remove-Item -LiteralPath $key -Force }
    if (Test-Path -LiteralPath $temporary) { [IO.Directory]::Delete($temporary) }
    Write-Output 'Closed owned native SSH tunnel and removed its temporary credential copy.'
}
exit $testExit
