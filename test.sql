connect;
create database db1;
use db1;
create table t1(a int, b int, c string(8), primary key (a));
create table t2(a int, b double, c string(8), primary key (a));
insert into t1(a, b, c) values 1, 1, 't1_1';
insert into t1(a, b, c) values 2, 2, 't1_2';
insert into t1(a, b, c) values 3, 3, 't1_3';
insert into t2(a, b, c) values 1, 1.0, 't2_1';
insert into t2(a, b, c) values 2, 2.718, 't2_2';
insert into t2(a, b, c) values 3, 3.141, 't2_3';
select * from t1 join t2 on t1.a < 3 where t1.b < t2.b;

-- 测试事务和
-- client 1: 
set auto commit false;
update t1 set c='cccccccc' where c='t1_1';
select * from t1;
commit;
delete from t2 where a=1;
select a from t2;

-- client 2:
connect;
use db1;
select * from t1;
select * from t2;

-- client 1:
commit;
