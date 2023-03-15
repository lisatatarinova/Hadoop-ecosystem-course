SET hive.query.name="tacles creation for task1";
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 150;

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

--drop database if exists tatarinova1 cascade;
USE tatarinovael;
DROP TABLE IF EXISTS UnpartitionedLogs;
DROP TABLE IF EXISTS Logs;
DROP TABLE IF EXISTS IPRegions;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Subnets;

--------UNPARTITIONED LOGS TABLE

CREATE EXTERNAL TABLE UnpartitionedLogs (
    ip STRING, --      1. Ip-адрес, с которого пришел запрос (STRING),
    request_time INT, --      2. Время запроса (TIMESTAMP или INT),
    request_http STRING, --      3. Пришедший с ip-адреса http-запрос (STRING),
    page_size INT, --      4. Размер переданной клиенту страницы (INT),
    status_code INT, --      5. Http-статус код (INT).
    client_metadata STRING --      6. Информация о клиентском приложении, с которого осуществлялся запрос на сервер, в том числе, информация о браузере (STRING).
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(\\d{8})\\d+\\s+((?:ht|f)tp://(?:www)?\\S+)\\s+(\\d+)\\s+(\\d{3})\\s+(.*)\\s+.*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';


--------PARTITIONED LOGS TABLE

CREATE EXTERNAL TABLE Logs (
    ip STRING,
    request_http STRING,
    page_size INT,
    status_code INT,
    client_metadata STRING
)
PARTITIONED BY (request_time INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (request_time)
SELECT ip, request_http, page_size, status_code, client_metadata, request_time FROM UnpartitionedLogs;

SELECT * FROM Logs LIMIT 10;


--------IPREGIONS TABLE

CREATE EXTERNAL TABLE IPRegions(
    ip STRING, -- 1. IP-адрес (STRING),
    region STRING -- 2. Регион (STRING).
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M/';

SELECT * FROM IPRegions LIMIT 10;


--------USERS TABLE

CREATE EXTERNAL TABLE Users(
    ip STRING, -- 1. IP-адрес (STRING),
    client_browser STRING, -- 2. Браузер пользователя (STRING),
    gender STRING, -- 3. Пол (STRING) //male, female,
    age INT -- 4. Возраст (INT).
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M/';

SELECT * FROM Users LIMIT 10;


--------SUBNETS TABLE

CREATE EXTERNAL TABLE Subnets(
    ip STRING,
    mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

SELECT * FROM Subnets LIMIT 10;
