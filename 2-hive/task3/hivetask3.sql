SET hive.query.name = "sql-request for task3";
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 150;
SET hive.auto.convert.join = false;

USE tatarinovael;


--show tables;
select  ipregion_new.region,
        if (gender = 'male', count(gender), 0) as male,
        if (gender = 'female', count(gender), 0) as female
from users
        inner join (select ip, region from IPRegions) ipregion_new on users.ip = ipregion_new.ip
        inner join (select ip from Logs) ip_in_logs on ip_in_logs.ip = users.ip
group by ipregion_new.region, gender
