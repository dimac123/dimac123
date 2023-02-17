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
