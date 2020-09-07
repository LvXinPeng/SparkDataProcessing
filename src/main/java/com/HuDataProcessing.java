package com;

import com.alibaba.fastjson.JSON;
import com.utils.DataFrame;
import com.utils.DataFrames;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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

import java.lang.reflect.Method;
import java.util.*;

/**
 * Hello world!
 */
public class HuDataProcessing {

    public static class SparkSessionSingleton {
        private static SparkSession instance;

        private SparkSessionSingleton(SparkSession sparkSession) {

        }

        public static synchronized SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession
                        .builder()
                        .config(sparkConf)
                        .enableHiveSupport()
                        .getOrCreate();
            }
            return instance;
        }
    }

    public static void main(String[] args) throws InterruptedException, NoSuchMethodException {
        Class<Hive> hiveClass = Hive.class;
        Method alterTable = hiveClass.getMethod("alterTable", String.class, Table.class, EnvironmentContext.class);
        Method alterTable2 = hiveClass.getMethod("alterTable", String.class, Table.class, boolean.class, EnvironmentContext.class);
        System.out.println(alterTable.getName());
        System.out.println(alterTable);
        System.out.println(alterTable2.getName());
        System.out.println(alterTable2);

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
                rowDataSet.registerTempTable("huRowData");
                Dataset<Row> extractDataSet = spark.sql("select *, debase64(payload) as data from huRowData");
//            extractDataSet.show();

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
                analysisDataSet.registerTempTable("huData");
                Dataset<Row> huDataSet = spark.sql("select client_id as vin, data.userId, data.account, " +
                        "data.longitude, data.latitude, data.speed, data.reportTime from huData");

                try {
                    huDataSet.show();
                } catch (Exception e) {
                    e.printStackTrace();
                    e.getCause();
                }
                // 写入hive
//                huDataSet.registerTempTable("hu");
//                spark.sql("insert into tmp.hu_position_analysis_tmp select * from hu");
                try {
                    huDataSet.write().format("hive").mode("append").saveAsTable("tmp.hu_position_analysis_tmp");
                } catch (Exception e) {
                    e.printStackTrace();
                }

//                huDataSet.write().insertInto("tmp.hu_position_analysis_tmp");
            }
//            spark.stop();
//            spark.close();

        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
