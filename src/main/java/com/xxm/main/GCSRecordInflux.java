package com.xxm.main;


import com.google.api.services.storage.model.ObjectAccessControl;
import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import org.apache.commons.collections.map.HashedMap;
import org.influxdb.dto.Point;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GCSRecordInflux implements Serializable {
    private double kurtosisOpen;
    private double kurtosisClose;
    private double skewnessOpen;
    private double skewnessClose;
    private double averageOpen;
    private double averageClose;
    private double stdOpen;
    private double stdClose;
    private Map<String,Double> fields;

    public GCSRecordInflux(){

    }

    public JavaPoint getJavaPoint(String tag, String measurement){
        Map<String,String> tags = new HashedMap();
        tags.put("tag",tag);
        return new JavaPoint(
                DateTime.now(),
                measurement,
                tags,
                new HashMap<String,Object> (fields)
        );
    }

    public GCSRecordInflux(Map<String,Double> map){
        this.fields = map;
        for (String key:map.keySet()){
            setValue(key,map.get(key));
        }
    }
    private void setValue(String key,double num){
        Random random = new Random();
        random.setSeed(5);
        if (key.equals("kurtosisOpen")){
            setKurtosisOpen(num*random.nextDouble());
        }else if (key.equals("kurtosisClose")){
            setKurtosisClose(num*random.nextDouble());
        }else if (key.equals("meanOpen")){
            setAverageOpen(num*random.nextDouble());
        }else if (key.equals("meanClose")){
            setAverageClose(num*random.nextDouble());
        }else if (key.equals("stdOpen")){
            setStdOpen(num*random.nextDouble());
        }else if (key.equals("stdClose")){
            setStdClose(num);
        }else if (key.equals("skewnessOpen")){
            setSkewnessOpen(num*random.nextDouble());
        }else{
            setSkewnessClose(num*random.nextDouble());
        }
    }
    public double getKurtosisOpen() {
        return kurtosisOpen;
    }

    public double getKurtosisClose() {
        return kurtosisClose;
    }

    public void setKurtosisClose(double kurtosisClose) {
        this.kurtosisClose = kurtosisClose;
    }

    public void setKurtosisOpen(double kurtosisOpen) {
        this.kurtosisOpen = kurtosisOpen;
    }

    public double getSkewnessOpen() {
        return skewnessOpen;
    }

    public void setSkewnessOpen(double skewnessOpen) {
        this.skewnessOpen = skewnessOpen;
    }

    public double getSkewnessClose() {
        return skewnessClose;
    }

    public void setSkewnessClose(double skewnessClose) {
        this.skewnessClose = skewnessClose;
    }

    public double getAverageOpen() {
        return averageOpen;
    }

    public void setAverageOpen(double averageOpen) {
        this.averageOpen = averageOpen;
    }

    public double getAverageClose() {
        return averageClose;
    }

    public void setAverageClose(double averageClose) {
        this.averageClose = averageClose;
    }

    public double getStdOpen() {
        return stdOpen;
    }

    public void setStdOpen(double stdOpen) {
        this.stdOpen = stdOpen;
    }

    public double getStdClose() {
        return stdClose;
    }

    public void setStdClose(double stdClose) {
        this.stdClose = stdClose;
    }

    public Map<String, Double> getFields() {
        return fields;
    }

    public void setFields(Map<String, Double> fields) {
        this.fields = fields;
    }
}
