package com.xxm.main;

public class TradingRecordReturn {

    private String symbol;
    private double high;
    private double low;
    private double close;
    private double open;
    private double volume;
    private String timestamp;

    public TradingRecordReturn(String SymbolID,double high,double low,double close,double open, double volume){
        this.symbol = SymbolID;
        this.high =high;
        this.low = low;
        this.close = close;
        this.open = open;
        this.volume = volume;
    }

  public TradingRecordReturn(){

  }

    public void setTimestamp(String timestampAsString) {

    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }
}
