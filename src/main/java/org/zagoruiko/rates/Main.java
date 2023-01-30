package org.zagoruiko.rates;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.zagoruiko.rates.client.BinanceClient;
import org.zagoruiko.rates.client.dto.ExchangeInfoDTO;
import org.zagoruiko.rates.client.dto.SymbolDTO;
import org.zagoruiko.rates.service.RatesSparkService;
import org.zagoruiko.rates.service.TradeHistorySparkService;
import org.zagoruiko.rates.writer.DatasetWriter;

import java.io.IOException;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@PropertySource(value = "classpath:application.properties")
public class Main {

    private static Format format = new SimpleDateFormat("yyyy-MM-dd");
    private static Calendar calendar = Calendar.getInstance();


    @Autowired
    @Qualifier("postgres.jdbc.url")
    private String jdbcUrl;

    @Autowired
    @Qualifier("postgres.jdbc.driver")
    private String jdbcDriver;

    @Autowired
    @Qualifier("postgres.jdbc.user")
    private String jdbcUser;

    @Autowired
    @Qualifier("postgres.jdbc.password")
    private String jdbcPassword;
    private RatesSparkService ratesSparkService;
    private TradeHistorySparkService tradeHistorySparkService;
    private SparkSession spark;

    private DatasetWriter ratesWriter;
    private JdbcTemplate jdbcTemplate;

    private BinanceClient binanceClient;

    @Autowired
    public void setSparkService(RatesSparkService ratesSparkService) {
        this.ratesSparkService = ratesSparkService;
    }

    @Autowired
    public void setBinanceClient(BinanceClient binanceClient) {
        this.binanceClient = binanceClient;
    }

    @Autowired
    public void setTradeHistorySparkService(TradeHistorySparkService tradeHistorySparkService) {
        this.tradeHistorySparkService = tradeHistorySparkService;
    }

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Autowired
    @Qualifier("pgJdbcWriter")
    public void setRatesWriter(DatasetWriter ratesWriter) {
        this.ratesWriter = ratesWriter;
    }


    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.spark = sparkSession;
    }
    public static void main(String[] args) throws IOException, ParseException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(Main.class.getPackage().getName());
        context.refresh();
        context.getBean(Main.class).run(args);
    }

    public void run(String[] args) {
        ratesSparkService.initCryptoRates();
        Dataset<Row> cryptoCurrencies = this.ratesSparkService.processExchangeCurrencies();
        ratesWriter.write(cryptoCurrencies, "trades.rates");

        ExchangeInfoDTO exchangeInfo = this.binanceClient.getExchangeInfo();

        //System.exit(0);
        ratesSparkService.initCurrenciesTables();
        ratesSparkService.initInvestingTables();
        ratesSparkService.repairCurrenciesTables();
        ratesSparkService.repairInvestingTables();


        Dataset<Row> united = ratesSparkService.processCurrencies("USD", "EUR", "UAH", "CZK", "BTC");
//        united.filter(functions.col("asset").equalTo("UAH")
//                        .and(functions.col("quote").equalTo("BTC"))
//                        .and(functions.col("date").equalTo("2022-05-04"))
//        ).show();
//        System.exit(0);
        ratesWriter.write(united, "expenses.rates");

        Dataset<Row> symbols = spark.createDataFrame(exchangeInfo.getSymbols(), SymbolDTO.class);
        ratesWriter.write(tradeHistorySparkService.loadTradeHistory(symbols), "trades.trade_history");
        ratesWriter.write(tradeHistorySparkService.loadPortfolio(symbols, new String[0]), "trades.portfolio_total");
        ratesWriter.write(tradeHistorySparkService.loadPortfolio(symbols, "exchange"), "trades.portfolio_by_exchange");
        ratesWriter.write(tradeHistorySparkService.loadPortfolio(symbols,
                functions.date_trunc("month", functions.col("date")).as("date")), "trades.portfolio_by_date");

        united.createOrReplaceTempView("mycurrencies3");
        this.spark.sql("SELECT asset, quote, max(date) max_date, min(date) min_date, max(rate) max_rate, min(rate) min_rate " +
                        "FROM mycurrencies3 GROUP BY asset, quote")
                .select(
                        functions.col("asset"),
                        functions.col("quote"),
                        functions.col("max_date"),
                        functions.col("min_date"),
                        functions.col("max_rate"),
                        functions.col("min_rate")
                ).orderBy(
                        functions.col("asset"),
                        functions.col("quote")
                ).show(500);

    }
}