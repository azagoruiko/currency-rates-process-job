package org.zagoruiko.rates.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Date;

public interface SparkService {

    void initCurrenciesTables();

    void initInvestingTables();

    void repairCurrenciesTables();

    void repairInvestingTables();

    Dataset<Row> selectRate();

    Dataset<Row> selectInvestingRate();

    Dataset<Row> selectInvestingRateAll();

    Dataset<Row> selectInvestingOverUSDTRate();

    Dataset processCurrencies();

    Date selectMaxDate(String source, String asset, String quote);
}
