package com.xxm.main;


import java.io.Serializable;
import java.util.List;

public class GCSRecord implements Serializable {
    private double openReturn;
    private double closeReturn;
    public GCSRecord(){

    }

    public GCSRecord(String[] line){
        this.openReturn = Double.parseDouble(line[0]);
        this.closeReturn = Double.parseDouble(line[1]);
    }

    public double getOpenReturn() {
        return openReturn;
    }

    public void setOpenReturn(double openReturn) {
        this.openReturn = openReturn;
    }

    public double getCloseReturn() {
        return closeReturn;
    }

    public void setCloseReturn(double closeReturn) {
        this.closeReturn = closeReturn;
    }

}
