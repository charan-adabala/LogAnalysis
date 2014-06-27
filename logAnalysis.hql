DROP TABLE log_analysis.accessLogData;
DROP TABLE log_analysis.secureLogData;
DROP TABLE log_analysis.accessCombinedLogData;

DROP DATABASE IF EXISTS log_analysis;

CREATE DATABASE log_analysis COMMENT 'Holds all log analysis project tables';

use log_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS accessLogData (
key STRING,
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE EXTERNAL TABLE IF NOT EXISTS secureLogData (
key STRING,
time_stamp STRING,
host_name STRING,
process_id STRING,
message STRING,
ip_address STRING,
port STRING,
ssh STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE EXTERNAL TABLE IF NOT EXISTS accessCombinedLogData (
key STRING,
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data inpath '/user/hduser/LogAnalysisOutput/AccessLog/part-m-00000' overwrite into table accessLogData;
load data inpath '/user/hduser/LogAnalysisOutput/SecureLog/part-m-00000' overwrite into table secureLogData;
load data inpath '/user/hduser/LogAnalysisOutput/AccessCombinedLog/part-m-00000' overwrite into table accessCombinedLogData;
