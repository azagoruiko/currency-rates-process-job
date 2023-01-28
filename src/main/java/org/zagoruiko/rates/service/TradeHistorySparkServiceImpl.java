package org.zagoruiko.rates.service;

import org.apache.spark.sql.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.zagoruiko.rates.util.ExchangeConsts.*;

@Service
public class TradeHistorySparkServiceImpl implements TradeHistorySparkService {
    //Date/Time	Trade ID	Type	Currency Pair	Amount	Price	Total	Fee type	Fee amount	Fee %

    private SparkSession spark;
    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    private Dataset<Row> loadExmoTradeHistory() {
        Dataset<Row> trades = spark.read().option("header", true).csv("s3a://trades/exmo/*");
        Dataset<Row> exmoTrades = trades
                .withColumn("date", functions.to_date(functions.col(EXMO.getDateCol()), EXMO.getDateFormat()))
                .withColumn("baseAsset", functions.split(functions.col(EXMO.getPairCol()), "_").getItem(0))
                .withColumn("quoteAsset", functions.split(functions.col(EXMO.getPairCol()), "_").getItem(1))
                .withColumn("price",
                        functions.col(EXMO.getPriceCol())
                                .cast("double"))
                .withColumn("executed",
                        functions.col(EXMO.getTotalCol())
                                .cast("double"))
                .withColumn("amount",
                        functions.col(EXMO.getAmountCol())
                                .cast("double").multiply(0.997))
                .withColumn("balance_asset",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getBuyStr()), functions.col("executed"))
                                .otherwise(functions.col("executed").multiply(-1))
                                .cast("double"))
                .withColumn("total_asset_purchased",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getBuyStr()), functions.col("executed"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("total_asset_sold",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getSellStr()), functions.col("executed"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("balance_quote",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getBuyStr()), functions.col("amount").multiply(-1))
                                .otherwise(functions.col("amount"))
                                .cast("double"))
                .withColumn("total_quote_returned",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getSellStr()), functions.col("amount"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("total_quote_spent",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getBuyStr()), functions.col("amount"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .select(functions.lit("exmo").as("exchange"),
                        functions.col("date"),
                        functions.col(EXMO.getPairCol()),
                        functions.col(EXMO.getSideCol()),
                        functions.col("executed"),
                        functions.col("amount"),
                        functions.col("total_asset_purchased"),
                        functions.col("total_asset_sold"),
                        functions.col("total_quote_spent"),
                        functions.col("total_quote_returned"),
                        functions.col("balance_asset"),
                        functions.col("balance_quote"),
                        functions.col("baseAsset"),
                        functions.col("quoteAsset")
                );
        return exmoTrades;
    }
    private Dataset<Row> loadBinanceTradeHistory(Dataset<Row> symbols) {
        Dataset<Row> trades = spark.read().option("header", true).csv("s3a://trades/binance/*");
        Dataset<Row> binanceTrades = trades
                .join(symbols, trades.col("Pair").equalTo(symbols.col("symbol")))
                .withColumn("date", functions.to_date(functions.col(BINANCE.getDateCol()), BINANCE.getDateFormat()))
                .withColumn("price",
                        functions.regexp_replace(functions.col("Price"), "[^0-9\\.]", "")
                                .cast("double"))
                .withColumn("executed",
                        functions.regexp_replace(functions.col("Executed"), "[^0-9\\.]", "")
                                .cast("double"))
                .withColumn("amount",
                        functions.regexp_replace(functions.col("Amount"), "[^0-9\\.]", "")
                                .cast("double").multiply(0.999))
                .withColumn("balance_asset",
                        functions.when(functions.col("Side").equalTo("BUY"), functions.col("executed"))
                                .otherwise(functions.col("executed").multiply(-1))
                                .cast("double"))
                .withColumn("total_asset_purchased",
                        functions.when(functions.col("Side").equalTo("BUY"), functions.col("executed"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("total_asset_sold",
                        functions.when(functions.col("Side").equalTo("SELL"), functions.col("executed"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("balance_quote",
                        functions.when(functions.col("Side").equalTo("BUY"), functions.col("amount").multiply(-1))
                                .otherwise(functions.col("amount"))
                                .cast("double"))
                .withColumn("total_quote_returned",
                        functions.when(functions.col("Side").equalTo("SELL"), functions.col("amount"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .withColumn("total_quote_spent",
                        functions.when(functions.col("Side").equalTo("BUY"), functions.col("amount"))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .select(functions.lit("binance").as("exchange"),
                        functions.col("date"),
                        functions.col(BINANCE.getPairCol()),
                        functions.col(BINANCE.getSideCol()),
                        functions.col("executed"),
                        functions.col("amount"),
                        functions.col("total_asset_purchased"),
                        functions.col("total_asset_sold"),
                        functions.col("total_quote_spent"),
                        functions.col("total_quote_returned"),
                        functions.col("balance_asset"),
                        functions.col("balance_quote"),
                        functions.col("baseAsset"),
                        functions.col("quoteAsset")
                );
        return binanceTrades;
    }

    @Override
    public Dataset<Row> loadTradeHistory(Dataset<Row> symbols) {
        Dataset<Row> binanceTrades = this.loadBinanceTradeHistory(symbols);
        Dataset<Row> exmoTrades = this.loadExmoTradeHistory();
        return binanceTrades.union(exmoTrades);
    }

    @Override
    public Dataset<Row> loadPortfolio(Dataset<Row> symbols, String ... additionalGroupingFields) {
        return this.loadPortfolio(symbols,
                Arrays.stream(additionalGroupingFields).map(x -> functions.col(x)).collect(Collectors.toList()).toArray(new Column[additionalGroupingFields.length]));
    }

    @Override
    public Dataset<Row> loadPortfolio(Dataset<Row> symbols, Column ... additionalGroupingFields) {
        List<Column> groupBy = new ArrayList<>();
        groupBy.add(functions.col("baseAsset"));
        groupBy.add(functions.col("quoteAsset"));
        groupBy.addAll(Arrays.asList(additionalGroupingFields));

        Dataset<Row> trades = loadTradeHistory(symbols);
        return trades
                .groupBy(groupBy.toArray(new Column[groupBy.size()]))
                .agg(
                        functions.sum("balance_asset").as("balance_asset"),
                        functions.sum("total_asset_purchased").as("total_asset_purchased"),
                        functions.sum("total_asset_sold").as("total_asset_sold"),
                        functions.sum("balance_quote").as("balance_quote"),
                        functions.sum("total_quote_spent").as("total_quote_spent"),
                        functions.sum("total_quote_returned").as("total_quote_returned"),
                        functions.abs(functions.sum("balance_quote").divide(functions.sum("balance_asset"))).as("avg_price"),
                        functions.abs(functions.sum("total_quote_spent").divide(functions.sum("total_asset_purchased"))).as("avg_buy_price"),
                        functions.abs(functions.sum("total_quote_returned").divide(functions.sum("total_asset_sold"))).as("avg_sell_price")
                );
                //.orderBy(groupBy.stream().map(x -> functions.col(x)).collect(Collectors.toList()).toArray(new Column[groupBy.size()]));
//                .filter(functions.col("quoteAsset").equalTo("USDT"))
//                .agg(functions.sum("balance_quote"))
//                .show(500);
//        System.exit(0);
    }
}
