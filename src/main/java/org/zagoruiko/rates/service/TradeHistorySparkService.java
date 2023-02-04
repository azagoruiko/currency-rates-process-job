package org.zagoruiko.rates.service;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TradeHistorySparkService {
    Dataset<Row> loadTradeHistory(Dataset<Row> symbols, Dataset<Row> rates);

    Dataset<Row> loadPortfolio(Dataset<Row> trades, Dataset<Row> symbols, String ... additionalGroupingFields);
    Dataset<Row> loadPortfolio(Dataset<Row> trades, Dataset<Row> symbols, Column... additionalGroupingFields);

    Dataset<Row> joinPnlToPortfolio(Dataset<Row> trades, Dataset<Row> rates);

}
