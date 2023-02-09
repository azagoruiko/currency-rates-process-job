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
    public Dataset<Row> loadMaxRates(Dataset<Row> rates) {
        rates.createOrReplaceTempView("m_rates");

        return spark.sql("SELECT mr.asset asset, mr.quote quote, mr.date as date, r.rate rate FROM\n" +
                "(SELECT MAX(date) as date, asset, quote FROM m_rates\n" +
                "GROUP BY asset, quote) mr\n" +
                "JOIN m_rates r ON r.asset = mr.asset AND r.quote = mr.quote AND mr.date = r.date");
    }
    @Override
    public Dataset<Row> loadTradeHistory(Dataset<Row> symbols, Dataset<Row> rates) {
        Dataset<Row> binanceTrades = this.loadBinanceTradeHistory(symbols);
        Dataset<Row> exmoTrades = this.loadExmoTradeHistory();
        Dataset<Row> trades = binanceTrades.union(exmoTrades);

        Dataset<Row> maxRates = loadMaxRates(rates);
        maxRates.createOrReplaceTempView("m_rates");

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
                "ru.r_rate as usdt_rate, " +
                "CASE WHEN t.quoteAsset = 'BTC' THEN t.balance_quote ELSE t.balance_asset * COALESCE(rb.r_rate, 1) END as btc_balance_quote, " +
                "CASE WHEN t.quoteAsset = 'USDT' THEN t.balance_quote ELSE t.balance_asset * COALESCE(ru.r_rate, 1) END as usdt_balance_quote, " +
                "CASE WHEN t.quoteAsset = 'BTC' THEN t.total_quote_spent ELSE t.total_asset_purchased * COALESCE(rb.r_rate, 1)END as btc_total_quote_spent, " +
                "CASE WHEN t.quoteAsset = 'USDT' THEN t.total_quote_spent ELSE t.total_asset_purchased * COALESCE(ru.r_rate, 1)END as usdt_total_quote_spent, " +
                "CASE WHEN t.quoteAsset = 'BTC' THEN t.total_quote_returned ELSE t.total_asset_sold * COALESCE(rb.r_rate, 1) END as btc_total_quote_returned, " +
                "CASE WHEN t.quoteAsset = 'USDT' THEN t.total_quote_returned ELSE t.total_asset_sold * COALESCE(ru.r_rate, 1) END as usdt_total_quote_returned " +
                " FROM trades t " +
                "LEFT JOIN rates rb " +
                "ON rb.r_date = t.t_date AND rb.quote = 'BTC' AND rb.asset = t.baseAsset " +
                "LEFT JOIN rates ru " +
                "ON ru.r_date = t.t_date AND ru.quote = 'USDT' AND ru.asset = t.baseAsset "
        )
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
                functions.col("btc_balance_quote"),
                functions.col("usdt_balance_quote"),
                functions.col("btc_total_quote_spent"),
                functions.col("usdt_total_quote_spent"),
                functions.col("btc_total_quote_returned"),
                functions.col("usdt_total_quote_returned")
        );
    }

}
