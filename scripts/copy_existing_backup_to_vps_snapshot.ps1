$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$src = Join-Path $root "db\backup\aedmi_dump.zip"
$destDir = Join-Path $root "db\vps-snapshot"
$dest = Join-Path $destDir "aedmi_db_dump.zip"

if (-not (Test-Path $src)) {
    throw "No existe $src — genera un dump o usa create_vps_db_snapshot.ps1"
}
New-Item -ItemType Directory -Force -Path $destDir | Out-Null
Copy-Item -LiteralPath $src -Destination $dest -Force
Write-Host "Copiado a $dest — el restore en Linux usa este ZIP si no hay aedmi-data.sql.gz"
