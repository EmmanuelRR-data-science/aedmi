$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$zipPath = Join-Path $PSScriptRoot "aedmi_dump.zip"
$tempSqlPath = Join-Path $PSScriptRoot "aedmi_dump.sql"
$containerName = "aedmi-sdd-cursor-db-1"

if (-not (Test-Path $zipPath)) {
    throw "No se encontró el archivo de respaldo: $zipPath"
}

Expand-Archive -Path $zipPath -DestinationPath $PSScriptRoot -Force

try {
    docker exec $containerName sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DROP SCHEMA IF EXISTS geoespacial CASCADE; CREATE SCHEMA geoespacial;"'
    Get-Content $tempSqlPath | docker exec -i $containerName sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    Write-Host "Restore completado desde $zipPath"
}
finally {
    if (Test-Path $tempSqlPath) {
        Remove-Item $tempSqlPath -Force
    }
}
