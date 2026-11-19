$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host 'No se encuentra Docker. Abre o instala Docker Desktop.' -ForegroundColor Red
    exit 1
}
docker info --format '{{.ServerVersion}}' | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Docker Desktop no esta iniciado. Abre Docker Desktop.' -ForegroundColor Red
    exit 1
}
New-Item -ItemType Directory -Force -Path 'EVIDENCIAS', 'RESULTADOS' | Out-Null
Write-Host 'Ejecutando con archivos ORIGINALES de josepgarcia/datos...' -ForegroundColor Cyan
$mount = $PSScriptRoot + ':/home/jovyan/work'
# Spark puede imprimir avisos normales en stderr.
$ErrorActionPreference = 'Continue'
docker run --rm -v $mount -w /home/jovyan/work quay.io/jupyter/pyspark-notebook `
    bash -lc 'python ejercicios_spark.py && python proyecto_stocks.py' 2>&1 |
    Tee-Object -FilePath 'EVIDENCIAS\ejecucion_completa.txt'
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Error de ejecucion. Consultar EVIDENCIAS\ejecucion_completa.txt.' -ForegroundColor Red
    exit 1
}
Write-Host 'LISTO. Revisa RESULTADOS y EVIDENCIAS.' -ForegroundColor Green
