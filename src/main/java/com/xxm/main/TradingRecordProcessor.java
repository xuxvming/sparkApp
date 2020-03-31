package com.xxm.main;

import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import com.pygmalios.reactiveinflux.spark.jawa.SparkInflux;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TradingRecordProcessor implements Serializable{

    private static final Logger LOGGER = LoggerFactory.getLogger(TradingRecordProcessor.class);
    private static final long WINDOW_TIME = 20;
    private static final long SLIDING_INTERVAL =10;

    private JavaDStream<String> messageStream;
    private Dataset<GCSRecord> dataset;
    private SQLContext context;

    public TradingRecordProcessor(JavaDStream<String> messageStream,Dataset<GCSRecord> dataset,SQLContext context){
        this.context = context;
        this.dataset = dataset;
        this.messageStream = messageStream;
    }

    public void process(){
        SparkInflux sparkInflux = new SparkInflux("final_year_project",3000);
        TradingRecord tradingRecord = new TradingRecord();
        JavaDStream<TradingRecord> originalStream = messageStream.map(tradingRecord::parseData);
        processReturn(originalStream,sparkInflux);
    }

    private void processReturn(JavaDStream<TradingRecord> stream, SparkInflux sparkInflux) {
        Encoder<GCSRecordInflux> gcsRecordEncoderInflux = Encoders.bean(GCSRecordInflux.class);
        Encoder<GCSRecord> gcsRecordEncoder = Encoders.bean(GCSRecord.class);
        AtomicBoolean flag = new AtomicBoolean(false);
        JavaDStream<GCSRecord> gcsRecordJavaDStream = stream.map(
                tradingRecord -> {
                    GCSRecord gcsRecord = new GCSRecord();
                    gcsRecord.setOpenReturn(tradingRecord.getOpen());
                    gcsRecord.setCloseReturn(tradingRecord.getClose());
                    gcsRecord.setId(tradingRecord.getId());
                    return gcsRecord;
                }
        );
        List<GCSRecordInflux> list = new ArrayList<>();
        JavaDStream<JavaPoint> javaPointJavaDStream = gcsRecordJavaDStream.window(Durations.seconds(WINDOW_TIME))
                .transform((Function<JavaRDD<GCSRecord>, JavaRDD<GCSRecordInflux>>) gcsRecordJavaRDD -> {
                    if (!gcsRecordJavaRDD.isEmpty()) {
                        Dataset<GCSRecord> tempDataSet = context.createDataset(gcsRecordJavaRDD.rdd(), gcsRecordEncoder);
                        dataset = dataset.union(tempDataSet);

                        Map<String, Double> map = new ConcurrentHashMap<>();
                        double avgOpenReturn = dataset.agg(functions.avg("openReturn")).toJavaRDD().first().getDouble(0);
                        double avgCloseReturn = dataset.agg(functions.avg("closeReturn")).toJavaRDD().first().getDouble(0);
                        double stdOpenReturn = dataset.agg(functions.stddev("openReturn")).toJavaRDD().first().getDouble(0);
                        double stdCloseReturn = dataset.agg(functions.stddev("CloseReturn")).toJavaRDD().first().getDouble(0);
                        map.put("avgOpenReturn", avgOpenReturn);
                        map.put("avgCloseReturn", avgCloseReturn);
                        map.put("stdCloseReturn", stdCloseReturn);
                        map.put("stdOpenReturn", stdOpenReturn);
                        map.put("volatilityOpen",avgOpenReturn/stdOpenReturn);
                        map.put("volatilityClose",avgCloseReturn/stdCloseReturn);
                        GCSRecordInflux gcsRecordInflux = new GCSRecordInflux(map);
                        list.add(gcsRecordInflux);
                    }
                    ArrayList<GCSRecordInflux> copy = new ArrayList<>(list);
                    list.clear();
                    return context.createDataFrame(copy, GCSRecordInflux.class).as(gcsRecordEncoderInflux).toJavaRDD();
                }).map(gcsRecordInlux -> gcsRecordInlux.getJavaPoint("return", "JPM_HISTORICAL"));
        javaPointJavaDStream.print();
        sparkInflux.saveToInflux(javaPointJavaDStream);

    }

}
