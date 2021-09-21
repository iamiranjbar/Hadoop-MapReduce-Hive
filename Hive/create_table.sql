CREATE EXTERNAL TABLE IF NOT EXISTS Stock_Exchange_Daily_tmp(
symbol STRING,
full_name STRING,
quantity BIGINT,
volume BIGINT,
value BIGINT,
yesterday_qnt BIGINT,
first_order_value INT,
last_order_value INT,
last_order_value_change FLOAT,
last_order_value_change_percent FLOAT,
close_price INT,
close_price_change FLOAT,
close_price_change_percent FLOAT,
min_price INT,
max_price INT,
symbol_date STRING 
) 
COMMENT 'Daily Trades Of Iranian Stocks - Aggregated By TSETMC'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/data/data_lake/stock'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS Stock_Exchange_Daily(
symbol STRING,
full_name STRING,
quantity BIGINT,
volume BIGINT,
value BIGINT,
yesterday_qnt BIGINT,
first_order_value INT,
last_order_value INT,
last_order_value_change FLOAT,
last_order_value_change_percent FLOAT,
close_price INT,
close_price_change FLOAT,
close_price_change_percent FLOAT,
min_price INT,
max_price INT,
symbol_date DATE 
) 
STORED AS TEXTFILE LOCATION '/data/main_table';

INSERT OVERWRITE TABLE default.stock_exchange_daily 
Select symbol, full_name, quantity, volume, value, yesterday_qnt, first_order_value, last_order_value, last_order_value_change,
last_order_value_change_percent, close_price, close_price_change, close_price_change_percent, min_price, max_price,
from_unixtime(unix_timestamp(symbol_date ,'yyyy/MM/dd'), 'yyyy-MM-dd') FROM default.stock_exchange_daily_tmp ;

DROP TABLE default.stock_exchange_daily_tmp ;
