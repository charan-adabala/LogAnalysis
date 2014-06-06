DROP DATABASE IF EXISTS log_analysis;

CREATE DATABASE log_analysis COMMENT 'Holds all log analysis project tables';

use log_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS accessLogData (
ip_address STRING,
time_stamp STRING,
request_type STRING,
product_url STRING,
request_protocol STRING,
protocol_version STRING,
respcode STRING,
bytes STRING,
url STRING,
browser1 STRING,
browser2 STRING,
browser3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE EXTERNAL TABLE IF NOT EXISTS secureLogData (
time_stamp STRING,
host_name STRING,
process_id STRING,
message STRING,
ip_address STRING,
port STRING,
ssh STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE EXTERNAL TABLE IF NOT EXISTS accessCombinedLogData (
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

load data inpath '/user/LogAnalysisOutput/AccessLog/part-m-00000' overwrite into table accessLogData;
load data inpath '/user/LogAnalysisOutput/SecureLog/part-m-00000' overwrite into table secureLogData;
load data inpath '/user/LogAnalysisOutput/AccessCombinedLog/part-m-00000' overwrite into table accessCombinedLogData;
