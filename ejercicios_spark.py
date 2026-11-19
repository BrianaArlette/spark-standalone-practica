"""Spark Standalone: ejercicios 1-7 (paginas 19-24 del documento).
Las entradas de archivos son EXCLUSIVAMENTE persons.csv del repositorio indicado.
Los valores de ejercicios 2-4 estan dados literalmente en el PDF.
"""
from pathlib import Path
from pyspark.sql import SparkSession

BASE = Path(__file__).resolve().parent
PERSONAS = (BASE / 'DATOS' / 'persons.csv').as_uri()


def titulo(n, texto):
    print('\n' + '=' * 65)
    print(f'EJERCICIO {n}: {texto}')
    print('=' * 65)


def analizar_persona(linea):
    # El CSV original tiene name,age,gender,salary: no tiene city.
    partes = linea.split(',')  # UNA sola separacion de la linea.
    if len(partes) != 4:
        return None
    try:
        nombre, edad, genero, salario = partes
        return (nombre.strip(), int(edad), genero.strip(), int(salario))
    except ValueError:
        return None


def main():
    titulo(1, 'SparkSession personalizada')
    spark = (SparkSession.builder
             .master('local[2]')
             .appName('MiPrimerAnalisis')
             .config('spark.executor.memory', '1g')
             .config('spark.sql.shuffle.partitions', '8')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext
    try:
        print('Nombre:', sc.appName)
        print('Memoria executor:', sc.getConf().get('spark.executor.memory'))
        print('Particiones shuffle:', spark.conf.get('spark.sql.shuffle.partitions'))
        print('Master:', sc.master)

        titulo(2, 'RDD de numeros 1 a 20')
        numeros = sc.parallelize(list(range(1, 21)), 2)
        print('Total de elementos:', numeros.count())
        print('Primeros 5:', numeros.take(5))
        print('Suma:', int(numeros.sum()))
        print('Maximo:', numeros.max(), 'Minimo:', numeros.min())

        titulo(3, 'map y filter (1 a 50)')
        originales = sc.parallelize(range(1, 51), 2)
        triplicados = originales.map(lambda x: x * 3)
        mayores_50 = triplicados.filter(lambda x: x > 50)
        print('Primeros 10 multiplicados:', triplicados.take(10))
        print('Cantidad mayores que 50:', mayores_50.count())
        print('Primeros 10 filtrados:', mayores_50.take(10))

        titulo(4, 'Pares clave-valor: ventas del PDF')
        ventas = [('Laptop', 1200), ('Mouse', 25), ('Laptop', 1100),
                  ('Teclado', 75), ('Mouse', 30), ('Laptop', 1300)]
        rdd_ventas = sc.parallelize(ventas, 2)
        total_por_producto = rdd_ventas.reduceByKey(lambda a, b: a + b)
        ventas_por_producto = rdd_ventas.mapValues(lambda _: 1).reduceByKey(lambda a, b: a + b)
        print('Totales:', sorted(total_por_producto.collect()))
        print('Numero de ventas:', sorted(ventas_por_producto.collect()))
        print('Mayor total:', total_por_producto.takeOrdered(1, key=lambda x: (-x[1], x[0]))[0])

        titulo(5, 'Leer persons.csv ORIGINAL y contar mayores de 30')
        rdd = sc.textFile(PERSONAS)
        print('Primera linea del archivo:', rdd.first())
        sin_encabezado = rdd.filter(lambda x: x.strip() and not x.startswith('name,age,'))
        personas = sin_encabezado.map(analizar_persona).filter(lambda x: x is not None).cache()
        print('Total de registros validos:', personas.count())
        # Se extraen nombre y edad, como pide el ejercicio.
        mayores = personas.map(lambda p: (p[0], p[1])).filter(lambda p: p[1] > 30)
        print('Personas mayores de 30:')
        for nombre, edad in mayores.collect():
            print(' ', nombre, '(' + str(edad) + ')')
        print('Total de mayores de 30:', mayores.count())
        print('Nota: el ejemplo del PDF menciona 2, el CSV original contiene 10.')

        titulo(6, 'Optimizar split() usando persons.csv ORIGINAL')
        print('Encabezado REAL: name,age,gender,salary')
        print('El PDF solicita city pero este archivo no tiene city.')
        print('Extraemos los cuatro campos existentes con un solo split():')
        for fila in personas.take(5):
            print(' ', fila)
        print('Mejora: una separacion por registro en lugar de repetirla por columna.')

        titulo(7, 'Correccion de errores')
        # Error 1: obtener SparkContext desde la SparkSession existente.
        contexto = spark.sparkContext
        # Error 2: file:/// y una ruta local absoluta (Path.as_uri()).
        archivo = PERSONAS
        rdd_corregido = contexto.textFile(archivo)
        # Error 3: usar take, no collect, si el archivo puede ser grande.
        print('Ruta:', archivo)
        print('Muestra segura:', rdd_corregido.take(5))
        # Error 4: spark.stop() se ejecuta en finally.
        print('Cuatro errores identificados y corregidos.')
        print('Error 1: obtener sc con spark.sparkContext.')
        print('Error 2: file:// y ruta absoluta.')
        print('Error 3: take(5) en lugar de collect() con archivos grandes.')
        print('Error 4: cerrar la sesion con spark.stop().')
    finally:
        spark.stop()
        print('\nSparkSession de ejercicios cerrada correctamente.')


if __name__ == '__main__':
    main()
