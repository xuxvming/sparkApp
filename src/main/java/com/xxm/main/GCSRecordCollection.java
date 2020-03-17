package com.xxm.main;

import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class GCSRecordCollection implements Serializable {

    public GCSRecordCollection(){

    }

    public GCSRecord calculateBasedOnHistorical(TradingRecord record, Dataset<GCSRecord> dataset){
        dataset.show();
        return new GCSRecord();
    }

//    public GCSRecordInflux getKurtosisAndSkewness (GCSRecord record){
//
//
//        GCSRecordInflux gcsRecordInflux = (GCSRecordInflux) record;
//////        dataset = dataset.union(newDataSet);
//////        Dataset<Row> kurtosisOpen = dataset.agg(functions.kurtosis("openReturn"));
//////        Dataset<Row> kurtosisClose = dataset.agg(functions.kurtosis("closeReturn"));
////        gcsRecordInflux.setKurtosisClose(kurtosisClose.first().getDouble(0));
////        gcsRecordInflux.setKurtosisOpen(kurtosisOpen.first().getDouble(0));
//        return gcsRecordInflux;
//    }


}
