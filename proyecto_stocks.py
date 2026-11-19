"""Proyecto Spark Standalone (paginas 24-31) adaptado al stocks.txt REAL.
URL del archivo: https://github.com/josepgarcia/datos/blob/main/stocks.txt
Columnas REALES: id,name,category,quantity,price.
NO contiene simbolo bursatil, fecha ni volumen de transacciones.
Se conserva archivo original sin agregar ni inventar datos.
"""
from pathlib import Path
import shutil
from pyspark.sql import SparkSession

BASE = Path(__file__).resolve().parent
ARCHIVO = BASE / 'DATOS' / 'stocks.txt'
RESULTADOS = BASE / 'RESULTADOS'


def parsear_linea(linea):
    """Devuelve (id, nombre, categoria, cantidad, precio) o None."""
    partes = linea.strip().split(',')
    if len(partes) != 5:
        return None
    try:
        id_producto, nombre, categoria, cantidad, precio = partes
        id_producto = int(id_producto)
        cantidad = int(cantidad)
        precio = float(precio)
        if id_producto <= 0 or not nombre.strip() or not categoria.strip() or cantidad < 0 or precio < 0:
            return None
        return (id_producto, nombre.strip(), categoria.strip(), cantidad, precio)
    except (ValueError, TypeError):
        return None


def guardar_texto(nombre, contenido):
    (RESULTADOS / nombre).write_text(contenido, encoding='utf-8')


def main():
    spark = (SparkSession.builder
             .master('local[2]')
             .appName('AnalisisStocks')  # Nombre pedido en PDF, aunque datos son inventario.
             .config('spark.executor.memory', '512m')
             .config('spark.sql.shuffle.partitions', '8')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        sc = spark.sparkContext
        print('=' * 72)
        print('PROYECTO SPARK - ANALISIS DEL INVENTARIO ORIGINAL stocks.txt')
        print('FUENTE: github.com/josepgarcia/datos (SIN DATOS INVENTADOS)')
        print('=' * 72)

        # PASO 1 - Carga de archivos locales.
        print('\nPASO 1. Configuracion y carga')
        print('Aplicacion:', sc.appName)
        print('Executor memory:', sc.getConf().get('spark.executor.memory'))
        ruta = ARCHIVO.resolve().as_uri()  # file:///ruta/absoluta
        print('Archivo:', ruta)
        rdd = sc.textFile(ruta)
        print('Primeras 5 lineas:')
        for linea in rdd.take(5):
            print(' ', linea)
        print('Particiones:', rdd.getNumPartitions())

        # PASO 2 - Un solo split y validacion de los campos REALES.
        print('\nPASO 2. Limpieza y parseo de inventario')
        inventario = rdd.map(parsear_linea).filter(lambda p: p is not None).cache()
        cantidad_registros = inventario.count()
        print('Registros validos:', cantidad_registros)
        if cantidad_registros == 0:
            raise RuntimeError('Archivo vacio o con columnas diferentes de id,name,category,quantity,price')

        # PASO 3 - En lugar de simbolos bursatiles: precio por CATEGORIA.
        print('\nPASO 3. Precio maximo, minimo y promedio por categoria')
        categoria_precio = inventario.map(lambda x: (x[2], x[4])).cache()
        precio_max = categoria_precio.reduceByKey(max)
        precio_min = categoria_precio.reduceByKey(min)
        precio_promedio = categoria_precio.aggregateByKey(
            (0.0, 0),
            lambda ac, p: (ac[0] + p, ac[1] + 1),
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        ).mapValues(lambda x: x[0] / x[1])
        maximos = dict(precio_max.collect())
        minimos = dict(precio_min.collect())
        promedios = dict(precio_promedio.collect())
        for cat in sorted(promedios):
            print(f'{cat}: minimo ${minimos[cat]:.2f}, maximo ${maximos[cat]:.2f}, promedio ${promedios[cat]:.2f}')

        # PASO 4 - No hay fechas/volumen de acciones; se suman UNIDADES por categoria.
        print('\nPASO 4. Stock por categoria y productos con mas unidades')
        categoria_cantidad = inventario.map(lambda x: (x[2], x[3]))
        stock_por_categoria = categoria_cantidad.reduceByKey(lambda a, b: a + b).cache()
        for cat, n in sorted(stock_por_categoria.collect(), key=lambda x: (-x[1], x[0])):
            print(f'{cat}: {n} unidades')
        top10_productos = inventario.map(lambda x: (x[1], x[3])).sortBy(lambda x: (-x[1], x[0])).take(10)
        print('Top 10 productos por cantidad disponible:')
        for nombre, cantidad in top10_productos:
            print(f'  {nombre}: {cantidad}')

        # PASO 5 - Diferencia relativa entre precios de distintos productos de la categoria.
        # NO es volatilidad bursatil: no hay serie historica ni fechas en el dataset.
        print('\nPASO 5. Variacion porcentual del RANGO de precios por categoria')
        estadisticas_precios = categoria_precio.aggregateByKey(
            (float('-inf'), float('inf')),
            lambda ac, p: (max(ac[0], p), min(ac[1], p)),
            lambda a, b: (max(a[0], b[0]), min(a[1], b[1]))
        ).cache()
        rango_porcentual = estadisticas_precios.mapValues(
            lambda x: (x[0] - x[1]) / x[1] * 100 if x[1] > 0 else 0.0
        ).cache()
        mayores_diferencias = rango_porcentual.sortBy(lambda x: (-x[1], x[0])).take(5)
        menores_diferencias = rango_porcentual.sortBy(lambda x: (x[1], x[0])).take(5)
        print('5 categorias con MAYOR diferencia relativa de precios:')
        for cat, p in mayores_diferencias:
            print(f'  {cat}: {p:.2f}%')
        print('5 categorias con MENOR diferencia relativa de precios:')
        for cat, p in menores_diferencias:
            print(f'  {cat}: {p:.2f}%')
        print('Nota: es dispersion de precios de productos, NO volatilidad en el tiempo.')

        # PASO 6 - Guardar resultados en disco y elaborar un resumen sin datos inventados.
        print('\nPASO 6. Guardado y resumen')
        RESULTADOS.mkdir(parents=True, exist_ok=True)
        for nombre in ('stock_por_categoria', 'estadisticas_precios_categoria'):
            ruta_salida = RESULTADOS / nombre
            if ruta_salida.exists():
                shutil.rmtree(ruta_salida)  # Borra solo las carpetas propias de salida anterior.
        stock_por_categoria.sortByKey().saveAsTextFile((RESULTADOS / 'stock_por_categoria').resolve().as_uri())
        estadisticas_precios.sortByKey().saveAsTextFile((RESULTADOS / 'estadisticas_precios_categoria').resolve().as_uri())

        numero_categorias = inventario.map(lambda x: x[2]).distinct().count()
        total_unidades = inventario.map(lambda x: x[3]).sum()
        categoria_mayor_promedio = sorted(promedios.items(), key=lambda x: (-x[1], x[0]))[0]
        categoria_mayor_stock = stock_por_categoria.takeOrdered(1, key=lambda x: (-x[1], x[0]))[0]
        categoria_mayor_rango = mayores_diferencias[0]
        resumen = '\n'.join([
            'RESUMEN EJECUTIVO - stocks.txt ORIGINAL DEL REPOSITORIO',
            'Archivo: inventario de productos; NO contiene movimientos bursatiles.',
            f'Total de registros procesados: {cantidad_registros}',
            f'Categorias unicas: {numero_categorias}',
            f'Unidades disponibles en inventario: {int(total_unidades)}',
            f'Categoria con mayor precio promedio: {categoria_mayor_promedio[0]} (${categoria_mayor_promedio[1]:.2f})',
            f'Categoria con mas unidades: {categoria_mayor_stock[0]} ({categoria_mayor_stock[1]} unidades)',
            f'Categoria con mayor diferencia de precios: {categoria_mayor_rango[0]} ({categoria_mayor_rango[1]:.2f}%)',
            'NO calculable: dia de mayor volumen bursatil (el archivo no tiene fecha).',
            'NO calculable: volatilidad historica de acciones (no hay cotizaciones).',
            ''
        ])
        print('\n' + resumen)
        guardar_texto('resumen_ejecutivo.txt', resumen)
        cabecera = 'categoria,precio_minimo,precio_maximo,precio_promedio,diferencia_porcentual\n'
        filas = [f'{cat},{minimos[cat]:.2f},{maximos[cat]:.2f},{promedios[cat]:.2f},{(maximos[cat]-minimos[cat])/minimos[cat]*100:.2f}'
                 for cat in sorted(promedios)]
        guardar_texto('estadisticas_por_categoria.csv', cabecera + '\n'.join(filas) + '\n')
        guardar_texto('top10_productos_por_cantidad.csv',
                      'producto,cantidad\n' + '\n'.join(f'{nombre},{cant}' for nombre, cant in top10_productos) + '\n')
        guardar_texto('stock_por_categoria.csv', 'categoria,unidades\n' +
                      '\n'.join(f'{cat},{cant}' for cat, cant in sorted(stock_por_categoria.collect())) + '\n')
        print('Archivos generados en RESULTADOS/.')
    finally:
        spark.stop()
        print('SparkSession del proyecto cerrada correctamente.')


if __name__ == '__main__':
    main()
