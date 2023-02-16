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
