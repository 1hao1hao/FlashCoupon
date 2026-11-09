param([switch]$Benchmark,[switch]$Faults)
$ErrorActionPreference='Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$arguments=@('scripts/verify.py')
if($Benchmark){$arguments+='--benchmark'}
if($Faults){$arguments+='--faults'}
python @arguments
exit $LASTEXITCODE
