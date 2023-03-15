ADD FILE ./script.py;

USE tatarinova1;

SELECT TRANSFORM(ip, request_time, request_http, page_size, status_code, client_metadata)
USING 'python3 ./script.py' AS (ip, request_time, request_http, page_size, status_code, client_metadata)
FROM logs
limit 10;
