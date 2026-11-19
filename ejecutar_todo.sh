#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
mkdir -p EVIDENCIAS RESULTADOS
docker run --rm -v "${PWD}:/home/jovyan/work" -w /home/jovyan/work \
  quay.io/jupyter/pyspark-notebook bash -lc \
  'python ejercicios_spark.py && python proyecto_stocks.py' \
  2>&1 | tee EVIDENCIAS/ejecucion_completa.txt
