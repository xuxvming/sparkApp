package com.xxm.main;

import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import com.pygmalios.reactiveinflux.spark.InfluxUtils;
import com.pygmalios.reactiveinflux.spark.jawa.SparkInflux;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
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
        //JavaDStream<TradingRecord> originalStream = messageStream.map(tradingRecord::parseData);
        JavaDStream<TradingRecord> returnStream  = messageStream.map(tradingRecord::parseData)
                .window(Durations.seconds(WINDOW_TIME),Durations.seconds(SLIDING_INTERVAL))
                .reduce((Function2<TradingRecord, TradingRecord, TradingRecord>) (current, previous) -> {
                    double highReturn = (current.getHigh() - previous.getHigh()) /previous.getHigh();
                    double lowReturn = (current.getLow() - previous.getLow()) /previous.getLow();
                    double openReturn = (current.getOpen() - previous.getOpen()) /previous.getOpen();
                    double closeReturn = (current.getClose() - previous.getClose()) /previous.getClose();
                    TradingRecord res = new TradingRecord(previous.getSymbol(),highReturn,lowReturn,openReturn,closeReturn,current.getVolume());
                    res.setTimestamp(current.getTimestampAsString());
                    return res;});

        JavaDStream<JavaPoint> InfluxReturnStream = returnStream.map(message ->tradingRecord.getInfluxPoint(message,"return"));

        sparkInflux.saveToInflux(InfluxReturnStream);

        processReturn(returnStream,sparkInflux);
    }

    private void processReturn(JavaDStream<TradingRecord> stream, SparkInflux sparkInflux) {
        InfluxDB client = InfluxDBFactory.connect("http://35.202.114.76:8086");

        Encoder<GCSRecordInflux> gcsRecordEncoderInflux = Encoders.bean(GCSRecordInflux.class);
        Encoder<GCSRecord> gcsRecordEncoder = Encoders.bean(GCSRecord.class);
        String[] functions = {"mean","std","kurtosis","skewness"};
        AtomicBoolean flag = new AtomicBoolean(false);
        JavaDStream<GCSRecord> gcsRecordJavaDStream = stream.map(
                tradingRecord -> {
                    GCSRecord gcsRecord = new GCSRecord();
                    gcsRecord.setOpenReturn(tradingRecord.getOpen());
                    gcsRecord.setCloseReturn(tradingRecord.getClose());
                    flag.set(true);
                    return gcsRecord;
                }
        );
        List<GCSRecordInflux> list = new ArrayList<>();
        JavaDStream<JavaPoint> javaPointJavaDStream = gcsRecordJavaDStream.window(Durations.minutes(1))
                .transform(new Function<JavaRDD<GCSRecord>, JavaRDD<GCSRecordInflux>>() {
            @Override
            public JavaRDD<GCSRecordInflux> call(JavaRDD<GCSRecord> gcsRecordJavaRDD) throws InterruptedException {
                JavaRDD<GCSRecord> rdd = gcsRecordJavaRDD;

                if (!gcsRecordJavaRDD.isEmpty()){
                    Dataset<GCSRecord> tempDataSet = context.createDataset(gcsRecordJavaRDD.rdd(),gcsRecordEncoder);
                    dataset.union(tempDataSet);

                    Map<String,Double> map = new ConcurrentHashMap<>();
                    for(String function:functions){
                        MyTask task = new MyTask(function,map);
                        task.start();
                        task.join();
                    }
                    GCSRecordInflux gcsRecordInflux = new GCSRecordInflux(map);
                    list.add(gcsRecordInflux);
                }
                ArrayList<GCSRecordInflux> copy = new ArrayList<>(list);
                list.clear();
                return context.createDataFrame(copy,GCSRecordInflux.class).as(gcsRecordEncoderInflux).toJavaRDD();
            }
        }).map(gcsRecordInlux -> gcsRecordInlux.getJavaPoint("return","JPM_HISTORICAL"));
        javaPointJavaDStream.print();
        sparkInflux.saveToInflux(javaPointJavaDStream);

    }




class MyTask extends Thread{
        private String function;
        private Map<String,Double> res;
        public MyTask(String function,Map<String,Double> res){
            this.function = function;
            this.res = res;
        }
        public void run(){
            switch (function) {
                case "mean": {
                    Dataset<Row> temp = dataset.agg(functions.avg("openReturn"));
                    temp.show();
                    JavaRDD<Row> javaDoubleRDDOpen = temp.toJavaRDD();
                    res.put(function + "Open", javaDoubleRDDOpen.first().getDouble(0));
                    JavaRDD<Row> javaDoubleRDDClose = dataset.agg(functions.avg("closeReturn")).toJavaRDD();
                    res.put(function + "Close", javaDoubleRDDClose.first().getDouble(0));
                }
                case "std": {
                    Dataset<Row> temp = dataset.agg(functions.stddev("openReturn"));
                    //temp.show();
                    JavaRDD<Row> javaDoubleRDDOpen = temp.toJavaRDD();
                    res.put(function + "Open", javaDoubleRDDOpen.first().getDouble(0));
                    JavaRDD<Row> javaDoubleRDDClose = dataset.agg(functions.stddev("closeReturn")).toJavaRDD();
                    res.put(function + "Close", javaDoubleRDDClose.first().getDouble(0));
                }
                case "kurtosis": {
                    Dataset<Row> temp = dataset.agg(functions.kurtosis("openReturn"));
                    //temp.show();
                    JavaRDD<Row> javaDoubleRDDOpen = temp.toJavaRDD();
                    res.put(function + "Open", javaDoubleRDDOpen.first().getDouble(0));
                    JavaRDD<Row> javaDoubleRDDClose = dataset.agg(functions.kurtosis("closeReturn")).toJavaRDD();
                    res.put(function + "Close", javaDoubleRDDClose.first().getDouble(0));
                }
                default: {
                    Dataset<Row> temp = dataset.agg(functions.skewness("openReturn"));
                    // temp.show();
                    JavaRDD<Row> javaDoubleRDDOpen = temp.toJavaRDD();
                    res.put(function + "Open", javaDoubleRDDOpen.first().getDouble(0));
                    JavaRDD<Row> javaDoubleRDDClose = dataset.agg(functions.skewness("closeReturn")).toJavaRDD();
                    res.put(function + "Close", javaDoubleRDDClose.first().getDouble(0));
                }
            }
        }
}


}
