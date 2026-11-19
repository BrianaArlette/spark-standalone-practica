SPARK STANDALONE - ENTREGA CON LOS ARCHIVOS ORIGINALES
=======================================================

IMPORTANTE: Esta version reemplaza completamente al ZIP anterior que tenia
acciones bursatiles simuladas. NO utilizamos esos datos.

FUENTE INDICADA EN EL PDF:
https://github.com/josepgarcia/datos
https://raw.githubusercontent.com/josepgarcia/datos/refs/heads/main/stocks.txt
https://raw.githubusercontent.com/josepgarcia/datos/refs/heads/main/persons.csv

DATOS/stocks.txt: 30 productos + cabecera, con:
  id,name,category,quantity,price
DATOS/persons.csv: 20 personas + cabecera, con:
  name,age,gender,salary

No se invento ningun registro. Ambos archivos corresponden a los contenidos
que aparecen publicados en los enlaces originales del PDF.

ADVERTENCIA IMPORTANTE PARA EL PROFESOR:
El PDF describe stocks.txt como si fueran movimientos de acciones con columnas
Simbolo,Fecha,Precio,Volumen, pero el stocks.txt indicado por el propio PDF
ES UN INVENTARIO. Por eso el codigo usa el archivo indicado y adapta las
operaciones posibles a productos/categorias. No se pueden calcular dia de mayor
volumen bursatil ni volatilidad historica porque NO existen fecha ni cotizaciones.
De forma similar, persons.csv NO tiene 'city' y su numero real de personas con
edad >30 NO coincide con el ejemplo de 2 personas del documento.

ARCHIVOS
- ejercicios_spark.py: los 7 ejercicios (paginas 19-24).
- proyecto_stocks.py: los 6 pasos del proyecto usando el inventario REAL.
- DATOS/stocks.txt, DATOS/persons.csv: archivos del repositorio indicado.
- cuaderno_spark.ipynb: entrada desde Jupyter para ejecutar ambos scripts.
- Reporte_Spark_Standalone.pdf: respuestas y resultados de referencia
  calculados independientemente, NO evidencias de Spark.
- ejecutar_todo.ps1: ejecutor de Windows con Docker Desktop.
- ejecutar_todo.sh: ejecutor para Linux/WSL con Docker Desktop.

COMO EJECUTAR EN WINDOWS
1. Descomprimir el ZIP COMPLETO (no abrir el codigo dentro del ZIP).
2. Abrir Docker Desktop y dejar que arranque.
3. Abrir la carpeta Spark_Standalone_Originales con VS Code.
4. Abrir Terminal > New Terminal (PowerShell).
5. Verificar que el directorio sea Spark_Standalone_Originales.
6. Ejecutar todo en UNA LINEA:
   powershell -ExecutionPolicy Bypass -File .\ejecutar_todo.ps1
7. La primera vez Docker podria descargar imagen de PySpark.
8. Al terminar, buscar:
    EVIDENCIAS/ejecucion_completa.txt
    RESULTADOS/resumen_ejecutivo.txt
    RESULTADOS/estadisticas_por_categoria.csv
    RESULTADOS/stock_por_categoria.csv
    RESULTADOS/top10_productos_por_cantidad.csv
    RESULTADOS/stock_por_categoria/part-*
    RESULTADOS/estadisticas_precios_categoria/part-*
9. Tomar capturas REALES de pantalla con los 7 ejercicios,
   primeros registros del proyecto, resultados de pasos 3-5 y resumen.

VALIDACIONES ESPERADAS CON LOS ARCHIVOS REALES
- Ejercicio 5: 20 registros de personas, 10 personas mayores de 30.
- Proyecto: 30 productos, 6 categorias, 879 unidades.
- Categoria de mayor precio promedio: Electronics (~586.66).
- Categoria con mas unidades: Food (450).
- Categoria con mayor diferencia porcentual entre precios: Food (2296%).

CUESTIONES PENDIENTES QUE DEBES VERIFICAR TU
- Que Docker ejecute el codigo sin errores en tu equipo.
- Que salgan los archivos de RESULTADOS y el log EVIDENCIAS.
- Que tus capturas sean de la ejecucion REAL de tu computadora.

No se afirma haber ejecutado Spark en la preparacion de esta entrega.

ACTUALIZACION DEL REPORTE:
- Reporte_Spark_Standalone_Editable.docx: documento redactado de forma natural,
  listo para insertar capturas autenticas cuando el codigo haya sido ejecutado.
- Reporte_Spark_Standalone.pdf: misma version como PDF sin capturas todavia.
- Guia_Capturas.txt: nueve capturas recomendadas, con instrucciones exactas.
- Los resultados descritos en el reporte son valores de REFERENCIA obtenidos
  sin ejecutar Spark. No afirmar ejecucion exitosa hasta comprobarla en Docker.
