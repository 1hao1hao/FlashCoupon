param([switch]$Benchmark,[switch]$Faults)
$ErrorActionPreference='Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$composeArgs=@('-f','compose.yaml','-f','compose.sharding.yaml')
function Compose([string[]]$command){ & docker compose @composeArgs @command; if($LASTEXITCODE -ne 0){throw "Command failed: $command"} }
New-Item -ItemType Directory -Force reports | Out-Null
Compose @('up','-d','--build','mysql','redis','broker','proxy','mq-topic')
# Stop the deployed consumer so the integration test owns its MQ consumer lifecycle.
Compose @('stop','app')
try {
 Compose @('run','--rm','toolbox','sh','-c','go test -race -count=1 -v ./... && go vet ./... && go run ./cmd/whitelist')
} finally { Compose @('up','-d','--build','app') }
if($Benchmark){
 foreach($mode in @('direct','async')){Compose @('run','--rm','k6','run','-e',"MODE=$mode",'-e','ITERATIONS=500','-e','VUS=30','/scripts/claims.js')}
 Compose @('run','--rm','k6','run','-e','MODE=async','-e','DUPLICATE=1','/scripts/claims.js')
 Compose @('run','--rm','k6','run','-e','MODE=async','-e','ITERATIONS=2000','-e','STOCK=1000','-e','VUS=100','-e','REPORT_NAME=async-contention','/scripts/claims.js')
 Compose @('run','--rm','k6','run','/scripts/batch.js')
}
if($Faults){& (Join-Path $PSScriptRoot 'recovery.ps1')}
Get-Content -Raw scripts/audit.sql | & docker compose @composeArgs exec -T mysql mysql -uroot -pflashcoupon control
if($LASTEXITCODE -ne 0){throw 'SQL audit failed'}
