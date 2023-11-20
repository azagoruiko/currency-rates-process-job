package org.zagoruiko.rates.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Date;

public interface RatesSparkService {

    void initCurrenciesTables();

    void initCryptoRates();

    void initInvestingTables();

    void repairCryptoRatesTables();

    void repairCurrenciesTables();

    void repairInvestingTables();

    Dataset<Row> selectRate();

    Dataset<Row> selectInvestingRate();

    Dataset<Row> selectInvestingRateAll();

    Dataset<Row> selectInvestingOverUSDTRate();

    Dataset<Row> selectCryptoRates();

    Dataset processCurrencies(String ... currencies);

    Dataset processExchangeCurrencies();

    Date selectMaxDate(String source, String asset, String quote);

    SparkSession getSpark();
}
