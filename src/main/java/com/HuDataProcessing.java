package com;

import com.alibaba.fastjson.JSON;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * Hello world!
 */
public class HDataProcessing {

    public static class SparkSessionSingleton {
        private static SparkSession instance;

        private SparkSessionSingleton(SparkSession sparkSession) {

        }

        public static synchronized SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession
                        .builder()
                        .config(sparkConf)
                        .config("hive.exec.dynamic.partition", true)
                        .config("hive.exec.dynamic.partition.mode", "nonstrict")
                        .enableHiveSupport()
                        .getOrCreate();
            }
            return instance;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 2) {
            System.out.println("The input parameters are invalid.");
            return;
        }

        SparkConf conf = new SparkConf()
                .setAppName("data_hive")
//                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        Map<String, Object> kafkaParams = new HashMap<>();
        //kafka ConsumerParams kafkaConsumer消费者参数

        kafkaParams.put("bootstrap.servers", args[0]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "atlas");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        //配置kafka topic
        Collection<String> topics = Collections.singletonList("data_collect_topic");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = directStream.map(consumerRecord -> consumerRecord.value());

        lines.foreachRDD(rdd -> {
            SparkSession spark = SparkSessionSingleton.getInstance(rdd.context().getConf());
            spark.udf().register("debase64", new UDF1<String, String>() {
                @Override
                public String call(String payload) {

                    byte[] bytes = Base64.decodeBase64(payload);
                    DataFrame dataFrame = DataFrames.fromBytes(bytes, "1234567890123456");
                    String body = dataFrame.getBody();
                    return JSON.parseObject(body).get("data").toString();
                }
            }, DataTypes.StringType);

            Dataset<Row> rowDataSet = spark.read().json(rdd);
            if (Arrays.toString(rowDataSet.columns()).contains("payload")
                    && Arrays.toString(rowDataSet.columns()).contains("client_id")) {
                rowDataSet.registerTempTable("hRowData");
                Dataset<Row> extractDataSet = spark.sql("select *, debase64(payload) as data from hRowData");


                JavaRDD<String> jsonDataSet = extractDataSet.toJSON().toJavaRDD().map(new Function<String, String>() {

                    @Override
                    public String call(String st) {
                        System.out.println(st);
                        String replaceFirst = st.replaceAll("\\\\\"", "\"")
                                .replaceFirst(":\"\\{", ":{")
                                .replaceFirst("\"} *$", "}");
                        System.out.println(replaceFirst);
                        return replaceFirst;
                    }
                });
                Dataset<Row> analysisDataSet = spark.read().json(jsonDataSet);
                analysisDataSet.registerTempTable("hData");
                Dataset<Row> hDataSet = spark.sql("select client_id as v, data.userId, data.account" +
                        ", data.longitude, data.latitude, data.speed, CAST(data.reportTime/1000 AS BIGINT) as reportTime from hData");

                try {
                    hDataSet.show();
                } catch (Exception e) {
                    e.printStackTrace();
                    e.getCause();
                }
                // 写入hive
                try {
//                    hDataSet.write().format("hive").mode("append").partitionBy("month").saveAsTable(args[1]);
                    hDataSet.registerTempTable("tmp_h_analysis_tmp");
                    spark.sql("insert into " + args[1] + " partition(month)" +
                            " select * , from_unixtime(unix_timestamp(),'yyyy-MM')from tmp_h_analysis_tmp");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
