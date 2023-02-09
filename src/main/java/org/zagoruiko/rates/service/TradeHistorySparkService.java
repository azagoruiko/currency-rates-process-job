package org.zagoruiko.rates.service;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TradeHistorySparkService {
    Dataset<Row> loadTradeHistory(Dataset<Row> symbols, Dataset<Row> rates);
    Dataset<Row> loadMaxRates(Dataset<Row> rates);

}
