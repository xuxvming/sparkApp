package com.xxm.main;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TradingRecord implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TradingRecord.class);

    private String symbol;
    private double high;
    private double low;
    private double close;
    private double open;
    private double volume;
    private String id;

    private ObjectMapper objectMapper = new ObjectMapper();

    public TradingRecord(){

    }
    public TradingRecord(String SymbolID,double high,double low,double close,double open, double volume){
        this.symbol = SymbolID;
        this.high =high;
        this.low = low;
        this.close = close;
        this.open = open;
        this.volume = volume;
    }

    private TradingRecord parseMessage(String message){
        return new TradingRecord();
    }

    public TradingRecord parseData(String messageBytes){
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return objectMapper.readValue(messageBytes,TradingRecord.class);
        } catch (IOException e) {
            LOGGER.error("Error deserialize incoming message ",e);
            return  null;
        }
    }

    public TradingRecord calculateReturn(TradingRecord current, TradingRecord previous){
        //{"volume":"931122","symbol":"JPM","high":"96.8200","low":"95.1400","close":"95.1600","open":"96.5700","timestamp":"2020-03-09 09:31:00"}
        double highReturn = (current.getHigh() - previous.getHigh()) /previous.getHigh();
        double lowReturn = (current.getLow() - previous.getLow()) /previous.getLow();
        double openReturn = (current.getOpen() - previous.getOpen()) /previous.getOpen();
        double closeReturn = (current.getClose() - previous.getClose()) /previous.getClose();
        TradingRecord res = new TradingRecord(previous.getSymbol(),highReturn,lowReturn,openReturn,closeReturn,current.getVolume());
        return res;
    }

    public String getSymbol() {
        return symbol;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public double getOpen() {
        return open;
    }

    public double getVolume() {
        return volume;
    }

    public JavaPoint getInfluxPoint(String message,String tag){
        TradingRecord tradingRecord = parseData(message);
        return getInfluxPoint(tradingRecord,tag);
    }

    public JavaPoint getInfluxPoint(TradingRecord tradingRecord,String tag){
        Map<String, Object> fields = new HashMap();
        Map<String,String> tags = new HashMap();
        fields.put("open",tradingRecord.getOpen());
        fields.put("close",tradingRecord.getClose());
        fields.put("high",tradingRecord.getHigh());
        fields.put("low",tradingRecord.getLow());
        fields.put("volume",tradingRecord.getVolume());
        tags.put("tag",tag);

        return new JavaPoint(
                DateTime.now(),
                "time_series"+tradingRecord.getSymbol(),
                tags,
                fields
        );
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
