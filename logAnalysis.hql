DROP DATABASE IF EXISTS log_analysis;

CREATE DATABASE log_analysis COMMENT 'Holds all log analysis project tables';

use log_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS logdata (
ip_address STRING,
time_stamp STRING,
request_type STRING,
product_url STRING,
request_protocol STRING,
protocol_version STRING,
respcode STRING,
bytes STRING,
url STRING,
browser STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

load data inpath '/user/hduser/LogAnalysisOutput/*' overwrite into table logdata;
