package com;

import com.alibaba.fastjson.JSON;
import com.utils.DataFrame;
import com.utils.DataFrames;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class HuDataProcessing {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("hu_data_hive")
//                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        Map<String, Object> kafkaParams = new HashMap<>();
        //kafka ConsumerParams kafkaConsumer消费者参数
        kafkaParams.put("bootstrap.servers", "10.160.242.166:9092,10.160.242.253:9092,10.160.242.21:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "atlas");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        //配置kafka topic
        Collection<String> topics = Collections.singletonList("hu_data_collect_topic");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = directStream.map(consumerRecord -> consumerRecord.value());

        lines.foreachRDD(rdd -> {
            SparkSession spark = SparkSession.builder()
//                .config("spark.sql.warehouse.dir", warehouseLocaion)
//                    .config("hive.exec.dynamic.partition", "true")
//                    .config("hive.exec.dynamic.partition.mode", "nonstrict")
//                    .config("hive.exec.max.dynamic.partitions", 2000)
                    .enableHiveSupport()
                    .getOrCreate();
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
            rowDataSet.registerTempTable("huRowData");
            Dataset<Row> extractDataSet = spark.sql("select client_id as vin, debase64(payload) as data from huRowData");

            JavaRDD<String> jsonDataSet = extractDataSet.toJSON().toJavaRDD().map(new Function<String, String>() {

                @Override
                public String call(String st) {
                    return st.replaceAll("\\\\\"", "\"")
                            .replaceFirst(":\"\\{", ":{")
                            .replaceFirst("\"} *$", "}");
                }
            });
            Dataset<Row> analysisDataSet = spark.read().json(jsonDataSet);
            analysisDataSet.registerTempTable("huData");
            Dataset<Row> huDataSet = spark.sql("select vin,data.* from huData");

            huDataSet.show();
            huDataSet.write().insertInto("tmp.hu_position_analysis_tmp");

            spark.close();

        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
