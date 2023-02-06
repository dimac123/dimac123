## Настройка и работа с базой данных Teradata Express

**Скачиваем Teradata, запуск на виртуальной машине**

Проверяем доступность БД, выводим ip адрес виртуальной машины для подключения

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-1.JPG)


**Подключаемся к Teradata через DBeaver**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-2.JPG)


**Скачиваем Teradata Tools and Utilities**

Устанавливаем ODBC Driver, BTEQ. Добавляем в PATH ссылку на установленный клиент. Запуск Command Prompt(терминал)
Авторизовываемся, SQL запрос на подсчет записей

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-3.JPG)


**IMPORT данных в Терадату через CLI**

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

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-4.JPG)

Результат испорта

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module6/6-5.JPG)
