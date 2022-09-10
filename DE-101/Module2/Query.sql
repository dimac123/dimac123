--уникальное количество заказов по которым были возвраты
select count(distinct orders.order_id) from orders, returns
where orders.order_id = returns.order_id
--или так
select count(distinct orders.order_id) from orders
RIGHT JOIN returns on returns.order_id=orders.order_id


--общее коилчество заказов
select count(distinct order_id) from orders

--количество заказов без возвратов
select count(distinct orders.order_id) from orders
where orders.order_id not in (select distinct order_id from returns)