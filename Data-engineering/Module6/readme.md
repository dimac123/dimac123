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
- `import_bteq.btq` - файл импорта

Файл `import_bteq.btq` 
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

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module5/VPC.JPG)

**Скачиваем Key pair, заходим в PuTTY, прописываем адрес виртуальноый машины и авторизовываемся по Key pair**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module5/VPC2.JPG)

**Подключаем балансер для виртуальных машин**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module5/VPC1.JPG)


## Создаем виртуальную машину в Microsoft Azure

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module5/VPC4.JPG)

**Azure Blob Storage. Создаем аккаунт+контейнер, загружаем файл и создаем доступ к нему**

https://dimac123.blob.core.windows.net/mynewcontainer/Sample%20-%20Superstore.xls?sp=r&st=2023-01-18T08:03:46Z&se=2023-01-18T16:03:46Z&spr=https&sv=2021-06-08&sr=b&sig=DpDBvjt9JXFLNkjxw8aJFdtbaRZEA6J22oUG79kl3rU%3D

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module5/VPC3.JPG)