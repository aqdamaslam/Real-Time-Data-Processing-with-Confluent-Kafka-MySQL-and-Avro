show databases;

create database kafka;

use kafka;

show tables;

create table IF NOT EXISTS product
(
	id int,
    name VARCHAR(50),
    category VARCHAR(50),
    price float,
    last_updated timestamp,
    constraint pk Primary Key (id)
);

insert into product values(1, 'car', 'veh', 20.5, '2038-01-19 03:14:07.499999');
insert into product values(2, 'car', 'veh', 20.5, '2038-01-19 03:14:07.499999');
insert into product values(4, 'car', 'veh', 20.5, '2038-01-19 03:14:08.499999');

select * from product;

SELECT id, name, category, price, last_updated FROM product WHERE last_updated > {last_read_timestamp};
