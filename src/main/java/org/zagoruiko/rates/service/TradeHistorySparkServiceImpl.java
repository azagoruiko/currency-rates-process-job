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
                .withColumn("side",
                        functions.when(functions.col(EXMO.getSideCol()).equalTo(EXMO.getBuyStr()), functions.lit("BUY"))
                                .otherwise(functions.lit("SELL")))
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
                        functions.col("side"),
                        functions.col("executed"),
                        functions.col("amount"),
                        functions.col("price"),
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
                .withColumn("side",
                        functions.when(functions.col(BINANCE.getSideCol()).equalTo(BINANCE.getBuyStr()), functions.lit("BUY"))
                                .otherwise(functions.lit("SELL")))
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
                        functions.when(functions.col("Side").equalTo("BUY"), functions.col("amount").multiply(-1))
                                .otherwise(functions.lit(0))
                                .cast("double"))
                .select(functions.lit("binance").as("exchange"),
                        functions.col("date"),
                        functions.col(BINANCE.getPairCol()),
                        functions.col("side"),
                        functions.col("executed"),
                        functions.col("amount"),
                        functions.col("price"),
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
    public Dataset<Row> loadTradeHistory(Dataset<Row> symbols, Dataset<Row> rates) {
        Dataset<Row> binanceTrades = this.loadBinanceTradeHistory(symbols);
        Dataset<Row> exmoTrades = this.loadExmoTradeHistory();
        Dataset<Row> trades = binanceTrades.union(exmoTrades);
        trades
                .withColumnRenamed("date", "t_date")
                .createOrReplaceTempView("trades");
        rates
                .withColumnRenamed("date", "r_date")
                .withColumnRenamed("rate", "r_rate")
                .createOrReplaceTempView("rates");

        return this.spark.sql("SELECT " +
                "t.exchange, " +
                "t.t_date as date, " +
                "t.side, " +
                "t.baseAsset, " +
                "t.quoteAsset, " +
                "t.executed, " +
                "t.amount, " +
                "t.price, " +
                "t.total_asset_purchased, " +
                "t.total_asset_sold, " +
                "t.total_quote_spent, " +
                "t.total_quote_returned, " +
                "t.balance_asset, " +
                "t.balance_quote, " +
                "rb.r_rate as btc_rate, " +
                "ru.r_rate as usdt_rate " +
                " FROM trades t " +
                "LEFT JOIN rates rb " +
                "ON rb.r_date = t.t_date AND rb.quote = 'BTC' AND rb.asset = t.baseAsset " +
                "LEFT JOIN rates ru " +
                "ON ru.r_date = t.t_date AND ru.quote = 'USDT' AND ru.asset = t.baseAsset "
        ).withColumn("btc_balance_asset",
                        functions.col("balance_asset")
                                .multiply(functions.col("btc_rate")))
            .withColumn("usdt_balance_asset",
                    functions.col("balance_asset")
                            .multiply(functions.col("usdt_rate")))
            .withColumn("btc_total_asset_purchased",
                    functions.col("total_asset_purchased")
                            .multiply(functions.col("btc_rate")))
            .withColumn("usdt_total_asset_purchased",
                    functions.col("total_asset_purchased")
                            .multiply(functions.col("usdt_rate")))
            .withColumn("btc_total_asset_sold",
                    functions.col("total_asset_sold")
                            .multiply(functions.col("btc_rate")))
            .withColumn("usdt_total_asset_sold",
                    functions.col("total_asset_sold")
                            .multiply(functions.col("usdt_rate")))
        .select(
                functions.col("exchange"),
                functions.col("date"),
                functions.col("side"),
                functions.col("baseAsset"),
                functions.col("quoteAsset"),
                functions.col("executed"),
                functions.col("amount"),
                functions.col("total_asset_purchased"),
                functions.col("total_asset_sold"),
                functions.col("total_quote_spent"),
                functions.col("total_quote_returned"),
                functions.col("balance_asset"),
                functions.col("balance_quote"),
                functions.col("price"),
                functions.col("btc_rate"),
                functions.col("usdt_rate"),
                functions.col("btc_balance_asset"),
                functions.col("usdt_balance_asset"),
                functions.col("btc_total_asset_purchased"),
                functions.col("usdt_total_asset_purchased"),
                functions.col("btc_total_asset_sold"),
                functions.col("usdt_total_asset_sold")
        );
    }

    @Override
    public Dataset<Row> loadPortfolio(Dataset<Row> trades, Dataset<Row> symbols, String ... additionalGroupingFields) {
        return this.loadPortfolio(trades, symbols,
                Arrays.stream(additionalGroupingFields).map(x -> functions.col(x)).collect(Collectors.toList()).toArray(new Column[additionalGroupingFields.length]));
    }

    @Override
    public Dataset<Row> loadPortfolio(Dataset<Row> trades, Dataset<Row> symbols, Column ... additionalGroupingFields) {
        List<Column> groupBy = new ArrayList<>();
        groupBy.add(functions.col("baseAsset"));
        groupBy.add(functions.col("quoteAsset"));
        groupBy.addAll(Arrays.asList(additionalGroupingFields));

        trades = trades
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
                        functions.abs(functions.sum("total_quote_returned").divide(functions.sum("total_asset_sold"))).as("avg_sell_price"),
                        functions.sum("btc_balance_asset").as("btc_balance_asset"),
                        functions.sum("usdt_balance_asset").as("usdt_balance_asset"),
                        functions.sum("btc_total_asset_purchased").as("btc_total_asset_purchased"),
                        functions.sum("usdt_total_asset_purchased").as("usdt_total_asset_purchased"),
                        functions.sum("btc_total_asset_sold").as("btc_total_asset_sold"),
                        functions.sum("usdt_total_asset_sold").as("usdt_total_asset_sold")
                );
        return trades;
    }

    @Override
    public Dataset<Row> joinPnlToPortfolio(Dataset<Row> trades, Dataset<Row> rates) {
        Dataset<Row> latestDates = rates
                .groupBy(functions.col("asset"), functions.col("quote"))
                .agg(functions.max("date").as("date"),
                        functions.col("asset"), functions.col("quote"));

        Dataset<Row> pnl = rates
                .join(latestDates,
                        rates.col("date").equalTo(latestDates.col("date"))
                        .and(rates.col("asset").equalTo(latestDates.col("asset")))
                        .and(rates.col("quote").equalTo(latestDates.col("quote"))))
                .join(trades,
                        rates.col("date").equalTo(latestDates.col("date"))
                                .and(trades.col("baseAsset").equalTo(latestDates.col("asset")))
                                .and(trades.col("quoteAsset").equalTo(latestDates.col("quote"))))
                ;
        return pnl
                .withColumn("pnl", (rates.col("rate").divide(functions.col("avg_price")).minus(functions.lit(1)))
                        .as("pnl"))
                .withColumn("safe_pnl", (rates.col("rate").divide(functions.col("avg_buy_price")).minus(functions.lit(1)))
                        .as("safe_pnl"))
                .select(
                rates.col("asset"),
                rates.col("quote"),
                rates.col("rate"),
                functions.col("pnl"),
                trades.col("avg_buy_price"),
                trades.col("avg_sell_price"),
                trades.col("avg_price"),
                functions.col("safe_pnl"),
                trades.col("balance_asset"),
                (trades.col("total_asset_purchased")
                    .multiply(functions.col("rate")))
                        .minus(trades.col("total_asset_purchased")
                                .multiply(functions.col("avg_buy_price")))
                        .as("quote_value_by_pnl"),
                (trades.col("balance_asset")
                        .multiply(functions.col("rate")))
                        .minus(trades.col("balance_asset")
                                .multiply(functions.col("avg_price")))
                        .as("quote_value_by_safe_pnl"),
                trades.col("total_asset_purchased"),
                trades.col("total_asset_sold"),
                trades.col("balance_quote"),
                trades.col("total_quote_spent"),
                trades.col("total_quote_returned"),
                trades.col("btc_balance_asset"),
                trades.col("usdt_balance_asset"),
                trades.col("btc_total_asset_purchased"),
                trades.col("usdt_total_asset_purchased"),
                trades.col("btc_total_asset_sold"),
                trades.col("usdt_total_asset_sold")
        );
    }
}
