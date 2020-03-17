package com.xxm.main;

import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.inject.internal.cglib.core.$AbstractClassGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkApp implements Serializable {

    public static void main(String[] args) throws InterruptedException {
        String projectID = "final-year-project-269315";
        String topic = "sparkapp";
        String checkpointDir = "/Users/xiuxuming/Desktop/FYP/sparkCheckPoint";
        String slidingInterval = "10000";
        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDir,()->createContext(projectID,topic,slidingInterval));
        context.start();
        context.awaitTermination();
    }

    private static JavaStreamingContext createContext(String projectID, String topic, String slidingInterval){
        SparkConf defaultSparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");

        SparkGCPCredentials credentials = new SparkGCPCredentials.Builder().jsonServiceAccount("/Users/xiuxuming/Desktop/FYP/SparkApp/src/main/resources/service-account.json").build();
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                defaultSparkConf,Duration.apply(Long.parseLong(slidingInterval)));

        Configuration hadoopConf = streamingContext.sparkContext().hadoopConfiguration();
        hadoopConf.set(
                EntriesCredentialConfiguration.BASE_KEY_PREFIX +
                        EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
                "true");
        hadoopConf.set("google.cloud.auth.service.account.json.keyfile", "/Users/xiuxuming/Desktop/FYP/SparkApp/src/main/resources/service-account.json");
        hadoopConf.set("fs.gs.project.id",projectID);

        SQLContext sparkContext = new SQLContext(streamingContext.sparkContext());
        SparkSession sparkSession = sparkContext.sparkSession();

        //String inputFile = "gs://dataproc-670dfbbb-c08b-407a-9ed4-64166c00a3e2-us-central1/processed/JPM_processed_gen/part-00000-8ad8ec40-6aa0-4d50-81c1-1139094a5d45-c000.csv";
        String inputFile = "gs://dataproc-670dfbbb-c08b-407a-9ed4-64166c00a3e2-us-central1/processed/JPM_processed_gen_return/part-00000-93802c85-37f1-4e88-ad01-b7b6aaee9489-c000.csv";

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header","true")
                .option("mode", "DROPMALFORMED")
                .load(inputFile);

        Encoder<GCSRecord> gcsRecordEncoder = Encoders.bean(GCSRecord.class);
        Dataset<GCSRecord> newDF = df.map((MapFunction<Row, GCSRecord>) row -> {
            String[] arr = new String[row.length()];
            Arrays.fill(arr,"0");
            for (int i =0 ; i<row.length();i++){
                if (row.get(i) != null){
                    arr[i] = row.get(i).toString();
                }
            }
            return new GCSRecord(arr);
        }, gcsRecordEncoder);

        //O(Nlog(N)) complexity
//        newDF = newDF.orderBy(newDF.col("index").desc());
//        newDF.persist(StorageLevel.DISK_ONLY());
        JavaDStream<String> messageStream = PubsubUtils.createStream(streamingContext,
                projectID,
                topic,
                "sparkapp",
                credentials,
                StorageLevel.MEMORY_AND_DISK()).map(message -> new String(message.getData()));
        messageStream.print();
        TradingRecordProcessor recordProcessor = new TradingRecordProcessor(messageStream,newDF,sparkContext);
        recordProcessor.process();
        return streamingContext;
    }

}
