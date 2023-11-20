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
import java.util.stream.Collectors;

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
        ExchangeInfoDTO exchangeInfo = this.binanceClient.getExchangeInfo();
        if (exchangeInfo == null || exchangeInfo.getSymbols() == null) {
            throw new RuntimeException("exchangeInfo = " + exchangeInfo);
        }

        ratesSparkService.initCryptoRates();
        Dataset<Row> cryptoCurrencies = this.ratesSparkService.processExchangeCurrencies();
        System.err.println(exchangeInfo.getSymbols().stream().map(s -> s.getBaseAsset() + " - " + s.getQuoteAsset()).collect(Collectors.joining(",")));
        Dataset<Row> symbols = ratesSparkService.getSpark().createDataFrame(exchangeInfo.getSymbols(), SymbolDTO.class);

        ratesWriter.write(cryptoCurrencies, "trades.rates");
        ratesWriter.write(tradeHistorySparkService.loadMaxRates(cryptoCurrencies), "trades.max_rates");


        ratesSparkService.initCurrenciesTables();
        ratesSparkService.initInvestingTables();
        ratesSparkService.repairCurrenciesTables();
        ratesSparkService.repairInvestingTables();


        Dataset<Row> united = ratesSparkService.processCurrencies("USD", "EUR", "UAH", "CZK", "BTC");
        ratesWriter.write(united, "expenses.rates");


        Dataset<Row> tradeHistory = tradeHistorySparkService.loadTradeHistory(symbols, cryptoCurrencies);
        ratesWriter.write(tradeHistory, "trades.trade_history");
    }
}