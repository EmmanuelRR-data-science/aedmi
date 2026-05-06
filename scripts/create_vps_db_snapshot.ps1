$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".env")) {
    throw "Falta .env en la raiz del repo."
}

$outDir = Join-Path $root "db\vps-snapshot"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$outFile = Join-Path $outDir "aedmi-data.sql.gz"

Write-Host "Generando respaldo dentro del contenedor db y copiando a $outFile ..."
docker compose exec db sh -lc 'pg_dump -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" --no-owner --no-acl --clean --if-exists | gzip -c > /tmp/aedmi-data.sql.gz'
if ($LASTEXITCODE -ne 0) { throw "pg_dump fallo" }

docker compose cp "db:/tmp/aedmi-data.sql.gz" $outFile
if ($LASTEXITCODE -ne 0) { throw "docker compose cp fallo" }

docker compose exec db rm -f /tmp/aedmi-data.sql.gz 2>$null

if (-not (Test-Path $outFile)) {
    throw "No se genero el archivo de salida."
}
$bytes = (Get-Item $outFile).Length
Write-Host "Listo: $outFile ($bytes bytes)"
Write-Host "Siguiente paso: git add db\vps-snapshot\aedmi-data.sql.gz"
