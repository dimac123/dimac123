## PySpark

**Установка и запуск Spark на Google colab**
```sh
 !pip install pyspark==3.0.1 py4j==0.10.9
```
Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/
Collecting pyspark==3.0.1
  Downloading pyspark-3.0.1.tar.gz (204.2 MB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 204.2/204.2 MB 5.4 MB/s eta 0:00:00
  Preparing metadata (setup.py) ... done
Collecting py4j==0.10.9
  Downloading py4j-0.10.9-py2.py3-none-any.whl (198 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 198.6/198.6 KB 18.0 MB/s eta 0:00:00
Building wheels for collected packages: pyspark
  Building wheel for pyspark (setup.py) ... done
  Created wheel for pyspark: filename=pyspark-3.0.1-py2.py3-none-any.whl size=204612244 sha256=9ee3b013bfed65c8efee99e7dda9162a06276f6b5586f326efc96f7c4d83d84f
  Stored in directory: /root/.cache/pip/wheels/ea/21/84/970b03913d0d6a96ef51c34c878add0de9e4ecbb7c764ea21f
Successfully built pyspark
Installing collected packages: py4j, pyspark
Successfully installed py4j-0.10.9 pyspark-3.0.1



![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-1.JPG)


**Подключаемся к Teradata через DBeaver**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-2.JPG)


**Скачиваем Teradata Tools and Utilities**

Устанавливаем ODBC Driver, BTEQ. Добавляем в PATH ссылку на установленный клиент. Запуск Command Prompt(терминал)
Авторизовываемся, SQL запрос на подсчет записей

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-3.JPG)


**IMPORT данных в Teradata через CLI**

- `returns.csv` - файл с данными
- `import_bteq.btq` - файл импорта:

```sh
.logon 192.168.1.117/dbc,dbc

DATABASE Superstore_st;

CREATE TABLE "Returns"
(Order_ID varchar(20), Returned varchar(3))
Primary Index (Order_id);


.import vartext ',' file = 'returns.csv', SKIP=1
.repeat*

USING  (Order_ID varchar(20), Returned varchar(3))

INSERT INTO "Returns" (Order_ID, Returned)
VALUES( :Order_ID, :Returned);
     
.QUIT
.LOGOFF
```

В CMD устанавливаем путь файла
"C:\Git\dimac123\Data-engineering\Module6"
И запуск файла
bteq .run file = import_bteq.btq

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-7.JPG)

Результат испорта

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-5.JPG)

**Подключение BI к Teradata**
Подключение успешно, данные доступны для построение отчетов

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-4.JPG)
