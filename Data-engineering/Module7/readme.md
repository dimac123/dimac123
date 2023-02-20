## Практика PySpark и SparkSQL (Dataframe API)

**Установка и запуск Spark на Google colab**
```sh
 !pip install pyspark==3.0.1 py4j==0.10.9
```

```sh
# cоздаём SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice 2").getOrCreate()

```sh
# проверяем запуск и версию
spark 
```

**Чтение текстовых данных**
```sh
# загружаем файл cars.csv
from google.colab import files
files.upload()
```
![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module7/7-1.JPG)

```sh
# читаем данные в DataFrame c автоматическим определение схемы:
df = spark.read.format("csv").option("header", "true").load("cars.csv")
```

```sh
# вывести в консоль 5 строк из датафрейма:
df.show(5)
```

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module7/7-2.JPG)

```sh
# Дополнительный параметр vertical=True выведет каждую строку данных построчно в виде колонка | значение
df.show(1, vertical=True)   
```
```sh
-RECORD 0-------------------------
 manufacturer_name | Subaru       
 model_name        | Outback      
 transmission      | automatic    
 color             | silver       
 odometer_value    | 190000       
 year_produced     | 2010         
 engine_fuel       | gasoline     
 engine_has_gas    | False        
 engine_type       | gasoline  
```

# Смотрим на DataFrame API

**.select()**

.select() выберет только нужные колонки (по аналогии с выражением SELECT в SQL)

```sh
# выбираем несколько колонок для отображения
df.select("manufacturer_name", "model_name", "transmission").show(5)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|           Subaru|   Outback|   automatic|
|           Subaru|   Outback|   automatic|
|           Subaru|  Forester|   automatic|
|           Subaru|   Impreza|  mechanical|
|           Subaru|    Legacy|   automatic|
+-----------------+----------+------------+
only showing top 5 rows
```

Можно обратиться к колонке через указание датафрейма:
```sh
df.select(df["manufacturer_name"], df["model_name"], df["transmission"]).show(5)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|           Subaru|   Outback|   automatic|
|           Subaru|   Outback|   automatic|
|           Subaru|  Forester|   automatic|
|           Subaru|   Impreza|  mechanical|
|           Subaru|    Legacy|   automatic|
+-----------------+----------+------------+
only showing top 5 rows
```
Или используя функцию колонки .col() из пакета functions
```sh
import pyspark.sql.functions as F
df.select(F.col("manufacturer_name"), F.col("model_name"), F.col("transmission")).show(5)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|           Subaru|   Outback|   automatic|
|           Subaru|   Outback|   automatic|
|           Subaru|  Forester|   automatic|
|           Subaru|   Impreza|  mechanical|
|           Subaru|    Legacy|   automatic|
+-----------------+----------+------------+
only showing top 5 rows
```

**.filter()**

Метод .filter() принимает условия для фильтрации:
```sh
# выберем только марки Audi
df.select("manufacturer_name", "model_name", "transmission").filter("manufacturer_name = 'Audi'").show(5)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|             Audi|        Q7|   automatic|
|             Audi|        TT|   automatic|
|             Audi|       100|  mechanical|
|             Audi|        A6|   automatic|
|             Audi|        Q3|   automatic|
+-----------------+----------+------------+
only showing top 5 rows
```

Цепочка условий:
```sh
# выберем только марки Audi и ручной коробкой
(
    df
    .select("manufacturer_name", "model_name", "transmission")
    .filter("manufacturer_name = 'Audi'")
    .filter("transmission = 'mechanical'")
    .show(5)
)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|             Audi|       100|  mechanical|
|             Audi|A6 Allroad|  mechanical|
|             Audi|       100|  mechanical|
|             Audi|        A4|  mechanical|
|             Audi|        80|  mechanical|
+-----------------+----------+------------+
only showing top 5 rows
```

Цепочка условий в виде одного SQL выражения:
```sh
(
    df
    .select("manufacturer_name", "model_name", "transmission")
    .filter("manufacturer_name = 'Audi' and transmission = 'mechanical'")
    .show(5)
)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|             Audi|       100|  mechanical|
|             Audi|A6 Allroad|  mechanical|
|             Audi|       100|  mechanical|
|             Audi|        A4|  mechanical|
|             Audi|        80|  mechanical|
+-----------------+----------+------------+
only showing top 5 rows
```

Удобнее и логичнее использовать col() для составления условий фильтрации:
```sh
(
    df
    .select("manufacturer_name", "model_name", "transmission")
    .filter(F.col("manufacturer_name") == "Audi")
    .filter(F.col("transmission") != "mechanical")
    .show(5)
)
```
```sh
+-----------------+----------+------------+
|manufacturer_name|model_name|transmission|
+-----------------+----------+------------+
|             Audi|        Q7|   automatic|
|             Audi|        TT|   automatic|
|             Audi|        A6|   automatic|
|             Audi|        Q3|   automatic|
|             Audi|        Q5|   automatic|
+-----------------+----------+------------+
only showing top 5 rows
```

**.count()**
Подсчет кол-ва строк:
```sh
df.count()
```
```sh
38531
```

Либо уникальных строк:
```sh
df.select("manufacturer_name").distinct().count()
```
```sh
55
```

**.groupBy() and .orderBy()**
Группировка и агрегация (аналог GROUP BY в SQL):

```sh
# сгрупировать по manufacturer_name и посчитать кол-во каждого
df.groupBy("manufacturer_name").count().show(5)
```
```sh
+-----------------+-----+
|manufacturer_name|count|
+-----------------+-----+
|       Volkswagen| 4243|
|         Infiniti|  162|
|          Peugeot| 1909|
|            Lexus|  213|
|           Jaguar|   53|
+-----------------+-----+
only showing top 5 rows
```

Сортировка по колонке по возрастанию:
```sh
df.groupBy("manufacturer_name").count().orderBy("count").show(5)
```
```sh
+-----------------+-----+
|manufacturer_name|count|
+-----------------+-----+
|          Lincoln|   36|
|       Great Wall|   36|
|              ЗАЗ|   42|
|          Pontiac|   42|
|         Cadillac|   43|
+-----------------+-----+
only showing top 5 rows
```

Сортировка по колонке по убыванию. Тут надо явно задать колонку через col():
```sh
df.groupBy("manufacturer_name").count().orderBy(F.col("count").desc()).show(5)
```
```sh
+-----------------+-----+
|manufacturer_name|count|
+-----------------+-----+
|       Volkswagen| 4243|
|             Opel| 2759|
|              BMW| 2610|
|             Ford| 2566|
|          Renault| 2493|
+-----------------+-----+
only showing top 5 rows

**.withColumnRenamed() and .withColumn()**
Переименовать существующую колонку:
```
df.withColumnRenamed("manufacturer_name", "manufacturer").select("manufacturer").show(5)
```sh
+------------+
|manufacturer|
+------------+
|      Subaru|
|      Subaru|
|      Subaru|
|      Subaru|
|      Subaru|
+------------+
only showing top 5 rows

Создать новую колонку. Первый аргумент это название новой колонки, второй агрумент это выражение (обязательно использовать col() если ссылаемся на другую колонку):
```
df.withColumn("next_year", F.col("year_produced") + 1).select("year_produced", "next_year").show(5)
```sh
+-------------+---------+
|year_produced|next_year|
+-------------+---------+
|         2010|   2011.0|
|         2002|   2003.0|
|         2001|   2002.0|
|         1999|   2000.0|
|         2001|   2002.0|
+-------------+---------+
only showing top 5 rows

**.printSchema() and .describe()**
Вывести схему датафрейма (типы колонок):
```
df.printSchema()
```sh
root
 |-- manufacturer_name: string (nullable = true)
 |-- model_name: string (nullable = true)
 |-- transmission: string (nullable = true)
 |-- color: string (nullable = true)
 |-- odometer_value: string (nullable = true)
 |-- year_produced: string (nullable = true)
 |-- engine_fuel: string (nullable = true)
 |-- engine_has_gas: string (nullable = true)
 
```
df.schema
```sh
StructType(List(StructField(manufacturer_name,StringType,true),StructField(model_name,StringType,true),StructField(transmission,StringType,true),StructField(color,StringType,true)
```

Вывести сводную статистику по датафрейму:
```
df.select("manufacturer_name", "model_name", "year_produced", "price_usd").describe().show()
```sh
+-------+-----------------+------------------+------------------+-----------------+
|summary|manufacturer_name|        model_name|     year_produced|        price_usd|
+-------+-----------------+------------------+------------------+-----------------+
|  count|            38531|             38531|             38531|            38531|
|   mean|             null|1168.2918056562726|2002.9437336170874|6639.971021255605|
| stddev|             null| 9820.119520829581| 8.065730511309935|6428.152018202911|
|    min|            Acura|               100|              1942|              1.0|
|    max|              УАЗ|            Таврия|              2019|           9999.0|
+-------+-----------------+------------------+------------------+-----------------+
```

```sh
#остановка сессии spark
spark.stop()
```

## Pipeline PySpark

**Задача. Сделать пайплайн обработки файла cars.csv.**

Необходимо посчитать по каждому производителю (поле manufacturer_name):

кол-во объявлений
средний год выпуска автомобилей
минимальную цену
максимальную цену
Выгрузить результат в output.csv

```sh
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as t


def extract_data(spark: SparkSession) -> DataFrame:
    path = "cars.csv"
    return spark.read.option("header", "true").csv(path)


def transform_data(df: DataFrame) -> DataFrame:
    output = (
        df
        .groupBy("manufacturer_name")
        .agg(
            F.count("manufacturer_name").alias("count_ads"),
            F.round(F.avg("year_produced")).cast(t.IntegerType()).alias("avg_year_produced"),
            F.min("price_usd").alias("min_price"),
            F.max("price_usd").alias("max_price"),
        )
        .orderBy(F.col("count_ads").desc())
    )
    return output


def save_data(df: DataFrame) -> None:
    df.coalesce(4).write.mode("overwrite").format("json").save("output.json")


def main():
    spark = SparkSession.builder.appName("Practice 2").getOrCreate()
    df = extract_data(spark)
    output = transform_data(df)
    save_data(output)
    #spark.stop()

main()
```

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module7/7-3.JPG)
