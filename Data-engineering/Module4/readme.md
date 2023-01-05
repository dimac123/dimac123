## Устанавливаем Pentaho на локальный компьютер
*Установка java (jdk+jre), загрузка драйвера JDBC, настройка переменных сред*

**Создаем подключение к БД**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho5.JPG)

В этой папке Module4 находятся:
- `staging orders.ktr` - трансформация, которая загружает данные из файлы Superstore в Postgres
- `dim_tables.ktr` и `sales_fact` - трансформация, которая трансформирует данные (T в ETL) внутри нашей базы данных
- `Pentaho Job.kjb` - главный job, который выполняет последовательность трансформаций (оркестрирует нашим data pipeline)

## Запуск трансформаций и job

**staging orders.ktr**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho.JPG)


**dim_tables.ktr**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho2.JPG)


**Pentaho Job.kjb**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho3.JPG)


**sales_fact**

![Иллюстрация к проекту](https://github.com/dimac123/dimac123/blob/main/Data-engineering/Module4/Pentaho4.JPG)
