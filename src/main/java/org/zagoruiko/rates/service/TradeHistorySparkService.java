package org.zagoruiko.rates.service;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TradeHistorySparkService {
    Dataset<Row> loadTradeHistory(Dataset<Row> symbols);

    Dataset<Row> loadPortfolio(Dataset<Row> symbols, String ... additionalGroupingFields);
    Dataset<Row> loadPortfolio(Dataset<Row> symbols, Column... additionalGroupingFields);
}
