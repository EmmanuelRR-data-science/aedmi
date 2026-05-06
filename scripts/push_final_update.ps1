# Publica la rama update-final en origin (ejecutar desde la raiz del repo).
# Requisitos: Git en PATH, credenciales GitHub configuradas, red.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$env:GIT_TERMINAL_PROMPT = "0"

git fetch origin 2>&1 | Write-Host
$has = git show-ref --verify --quiet refs/heads/update-final 2>$null
if ($LASTEXITCODE -ne 0) {
    git checkout -b update-final
} else {
    git checkout update-final
}

# Respaldos: copiar ZIP heredado si aun no hay snapshot comprimido
$dataGz = "db\vps-snapshot\aedmi-data.sql.gz"
$zipSnap = "db\vps-snapshot\aedmi_db_dump.zip"
if (-not (Test-Path $dataGz)) {
    $legacy = "db\backup\aedmi_dump.zip"
    if ((Test-Path $legacy) -and -not (Test-Path $zipSnap)) {
        New-Item -ItemType Directory -Force -Path "db\vps-snapshot" | Out-Null
        Copy-Item -LiteralPath $legacy -Destination $zipSnap -Force
        Write-Host "Copiado respaldo legacy a $zipSnap"
    }
}

git add -A
git reset HEAD -- .env .env.prod 2>$null
if (Test-Path "frontend\node_modules") { git reset HEAD -- frontend\node_modules 2>$null }
if (Test-Path "frontend\.next") { git reset HEAD -- frontend\.next 2>$null }

git status --short
$staged = git diff --cached --name-only
if (-not $staged) {
    Write-Host "Nada que commitear."
    exit 0
}

git commit -m "chore: snapshot VPS, restore ETL config y plantilla PPTX"
git push -u origin update-final
Write-Host "Listo. Remoto:" (git remote get-url origin)
