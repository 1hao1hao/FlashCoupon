$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$composeArgs = @('-f','compose.yaml','-f','compose.sharding.yaml')
$base = 'http://localhost:8080'
$adminKey = if ($env:ADMIN_KEY) { $env:ADMIN_KEY } else { 'dev-admin-key' }
$userKey = if ($env:USER_KEY) { $env:USER_KEY } else { 'dev-user-key' }
$admin = @{'X-Admin-Key'=$adminKey}
function Token([long]$userId) {
    $expires = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()+3600
    $payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${userId}:${expires}")).TrimEnd('=').Replace('+','-').Replace('/','_')
    $hmac = New-Object Security.Cryptography.HMACSHA256
    $hmac.Key = [Text.Encoding]::UTF8.GetBytes($userKey)
    $signature = ([BitConverter]::ToString($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)))).Replace('-','').ToLowerInvariant()
    $hmac.Dispose()
    return "$payload.$signature"
}
function Compose([string[]]$command) {
    & docker compose @composeArgs @command
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $command" }
}
$now=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$template = Invoke-RestMethod "$base/admin/templates" -Method Post -Headers $admin -ContentType 'application/json' -Body (@{merchant_id=$now;name='fault-recovery';stock=20;per_user=1;threshold=1000;discount=100;starts_at=$now-1;ends_at=$now+900;valid_until=$now+3600;mode='async'} | ConvertTo-Json)
$watch=[Diagnostics.Stopwatch]::StartNew()
try {
    Compose @('stop','broker')
    for ($i=1;$i -le 20;$i++) {
        $headers=@{Authorization="Bearer $(Token ($now*100+$i))"}
        $body=@{merchant_id=$template.merchant_id;template_id=$template.id;request_id="recovery-$i"} | ConvertTo-Json
        $accepted=Invoke-RestMethod "$base/v1/claims" -Method Post -Headers $headers -ContentType 'application/json' -Body $body
        if ($accepted.status -ne 'accepted') { throw 'Claim not accepted during broker outage' }
    }
    $audits=Invoke-RestMethod "$base/admin/audit" -Headers $admin
    $before=$audits | Where-Object { $_.template_id -eq $template.id }
    if ($before.pending -ne 20 -or $before.issued -ne 0) { throw "Unexpected outage inventory: $($before | ConvertTo-Json -Compress)" }
    $pending = & docker compose @composeArgs exec -T redis redis-cli XPENDING fc:outbox relay
    if ([int]$pending[0] -lt 1) { throw 'No Stream PEL entry observed; fault did not cover pending recovery' }
    Compose @('stop','app')
    Compose @('restart','redis')
    Compose @('start','broker','app')
    $recovery=[Diagnostics.Stopwatch]::StartNew()
    $after=$null
    while ($recovery.Elapsed.TotalSeconds -lt 180) {
        try {
            $audits=Invoke-RestMethod "$base/admin/audit" -Headers $admin
            $after=$audits | Where-Object { $_.template_id -eq $template.id }
            if ($after.pending -eq 0 -and $after.issued -eq 20 -and $after.projected -eq 20 -and $after.consistent) { break }
        } catch { Write-Host 'Waiting for restarted services...' }
        Start-Sleep -Seconds 1
    }
    if (!$after -or $after.pending -ne 0 -or $after.issued -ne 20 -or $after.projected -ne 20 -or !$after.consistent) { throw 'Fault recovery did not converge' }
    $report=@{scenario='broker outage + relay/app stop + Redis AOF restart + pending reclaim';accepted=20;observed_stream_pending=[int]$pending[0];recovery_seconds=$recovery.Elapsed.TotalSeconds;total_seconds=$watch.Elapsed.TotalSeconds;before=$before;after=$after}
    $report | ConvertTo-Json -Depth 6 | Set-Content reports/recovery.json -Encoding UTF8
    $report | ConvertTo-Json -Depth 6
} finally {
    Compose @('start','redis','broker','app')
}
