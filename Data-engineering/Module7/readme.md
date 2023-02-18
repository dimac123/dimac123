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
```
```sh
```
```sh
```
```sh
```
```sh
```
