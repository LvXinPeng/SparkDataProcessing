### GPS数据处理
#### Kafka --> SparkStreaming --> Hive


##### UAT EXEC
    spark-submit 
    --master yarn 
    --class com.HuDataProcessing  
    --principal hive  
    --keytab /root/keytabs/hive.keytab  
    --driver-memory 1g 
    --driver-cores 1 
    --num-executors 30 
    --executor-memory 2g 
    --executor-cores 2  
    --conf "spark.default.parallelism=250"
    xinpeng-1.0-SNAPSHOT-jar-with-dependencies.jar
    "kafka bootstrap" 
    "hive table"


#### Create Hive table
     CREATE TABLE database.tb (
     vin string,
     userId string,
     account string,
     longitude string, 
     latitude string,
     speed double,
     reportTime bigint ) 
     PARTITIONED BY (month string) 
     ROW FORMAT DELIMITED 
     FIELDS TERMINATED BY '|' 
     STORED AS PARQUET;