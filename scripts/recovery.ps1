$ErrorActionPreference='Stop'
Set-Location (Join-Path $PSScriptRoot '..')
python scripts/faults.py
exit $LASTEXITCODE
