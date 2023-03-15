SET hive.query.name = "sql-request for task2";
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 150;

USE tatarinovael;

--SHOW TABLES;

select request_time, count(request_time) as numberOfVisits
from logs
group by request_time
order by numberOfVisits desc
