В этой папке Module4 находятся:

- `Sample - Superstore` - файл с данными


Pentaho
- `staging orders.ktr` - трансформация, которая загружает данные из файлы Superstore в Postgres
- `dim_tables.ktr` и `sales_fact.ktr` - трансформация, которая трансформирует данные (T в ETL) внутри нашей базы данных
- `Pentaho Job.kjb` - главный job, который выполняет последовательность трансформаций (оркестрирует нашим data pipeline)

Alteryx 
- `Workflow sales.yxmd` - трансформация, которая загружает данные из файлы Superstore в Postgres





## Устанавливаем Pentaho на локальный компьютер
*Установка java (jdk+jre), загрузка драйвера JDBC, настройка переменных сред*

**Создаем подключение к БД**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho5.JPG)


## Запуск job и трансформаций

**staging orders.ktr**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho.JPG)


**dim_tables.ktr**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho2.JPG)


**sales_fact.ktr**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho4.JPG)


**Pentaho Job.kjb**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho3.JPG)

## Устанавливаем Alteryx на локальный компьютер


## Запуск Workflow с трансформаций (Join sheets, сумма+группировка)
![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Alteryx.png)
