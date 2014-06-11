/home/hduser/hive/bin/hive --auxpath /home/hduser/hive/lib/hive-hbase-handler-0.9.0.jar,/home/hduser/hive/lib/hbase-0.92.0.jar,/home/hduser/hive/lib/zookeeper-3.4.3.jar,/home/hduser/hive/lib/guava-r09.jar  -hiveconf hbase.zookeeper.quorum=Master

USE log_analysis;

DROP TABLE hbase_accessLogData;
CREATE TABLE hbase_accessLogData(key string,ip_address string,time_stamp string,request_type string,product_url string,request_protocol string,protocol_version string,respcode string,bytes string,url string,browser1 string,browser2 string,browser3 string) STORED BY'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,details:ip_address,details:time_stamp,details:request_type,details:product_url,details:request_protocol,details:protocol_version,details:respcode,details:bytes,details:url,details:browser1,details:browser2,details:browser3") TBLPROPERTIES ("hbase.table.name" = "hbaseAccessLogData");


INSERT OVERWRITE TABLE hbase_accesslogdata select  t.key,t.ip_address,t.time_stamp,t.request_type,t.product_url,t.request_protocol,t.protocol_version,t.respcode,t.bytes,t.url,t.browser1,t.browser2,t.browser3 from accessLogData t;


DROP TABLE hbase_accessCombinedLogData;
CREATE TABLE hbase_accessCombinedLogData(key string,ip_address string,time_stamp string,request_type string,product_url string,request_protocol string,protocol_version string,respcode string,bytes string,url string,browser string) STORED BY'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,details:ip_address,details:time_stamp,details:request_type,details:product_url,details:request_protocol,details:protocol_version,details:respcode,details:bytes,details:url,details:browser") TBLPROPERTIES ("hbase.table.name" = "hbaseAccessCombinedLogData");

INSERT OVERWRITE TABLE hbase_accessCombinedLogData select  t.key,t.ip_address,t.time_stamp,t.request_type,t.product_url,t.request_protocol,t.protocol_version,t.respcode,t.bytes,t.url,t.browser from accessCombinedLogData t;

DROP TABLE hbase_secureLogData;
CREATE TABLE hbase_secureLogData(key string,time_stamp string,host_name string,process_id string,message string,ip_address string,port string,ssh string) STORED BY'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,details:time_stamp,details:host_name,details:process_id,details:message,details:ip_address,details:port,details:ssh") TBLPROPERTIES ("hbase.table.name" = "hbaseSecureLogData");

INSERT OVERWRITE TABLE hbase_secureLogData select  t.key,t.ip_address,t.time_stamp,t.host_name,t.process_id,t.message,t.port,t.ssh from secureLogData t;
