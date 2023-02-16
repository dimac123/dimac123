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
+-----------------+----------+------------+------+--------------+-------------+-----------+--------------+-----------+---------------+---------+------------+-----+----------+---------+---------------+---------------+----------------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------------+
|manufacturer_name|model_name|transmission| color|odometer_value|year_produced|engine_fuel|engine_has_gas|engine_type|engine_capacity|body_type|has_warranty|state|drivetrain|price_usd|is_exchangeable|location_region|number_of_photos|up_counter|feature_0|feature_1|feature_2|feature_3|feature_4|feature_5|feature_6|feature_7|feature_8|feature_9|duration_listed|
+-----------------+----------+------------+------+--------------+-------------+-----------+--------------+-----------+---------------+---------+------------+-----+----------+---------+---------------+---------------+----------------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------------+
|           Subaru|   Outback|   automatic|silver|        190000|         2010|   gasoline|         False|   gasoline|            2.5|universal|       False|owned|       all|  10900.0|          False|   Минская обл.|               9|        13|    False|     True|     True|     True|    False|     True|    False|     True|     True|     True|             16|
|           Subaru|   Outback|   automatic|  blue|        290000|         2002|   gasoline|         False|   gasoline|            3.0|universal|       False|owned|       all|   5000.0|           True|   Минская обл.|              12|        54|    False|     True|    False|    False|     True|     True|    False|    False|    False|     True|             83|
|           Subaru|  Forester|   automatic|   red|        402000|         2001|   gasoline|         False|   gasoline|            2.5|      suv|       False|owned|       all|   2800.0|           True|   Минская обл.|               4|        72|    False|     True|    False|    False|    False|    False|    False|    False|     True|     True|            151|
|           Subaru|   Impreza|  mechanical|  blue|         10000|         1999|   gasoline|         False|   gasoline|            3.0|    sedan|       False|owned|       all|   9999.0|           True|   Минская обл.|               9|        42|     True|    False|    False|    False|    False|    False|    False|    False|    False|    False|             86|
|           Subaru|    Legacy|   automatic| black|        280000|         2001|   gasoline|         False|   gasoline|            2.5|universal|       False|owned|       all|  2134.11|           True|Гомельская обл.|              14|         7|    False|     True|    False|     True|     True|    False|    False|    False|    False|     True|              7|
+-----------------+----------+------------+------+--------------+-------------+-----------+--------------+-----------+---------------+---------+------------+-----+----------+---------+---------------+---------------+----------------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------------+
only showing top 5 rows

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module7/7-2.JPG)
