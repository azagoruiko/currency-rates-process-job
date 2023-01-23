package org.zagoruiko.rates;

import org.apache.spark.sql.*;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.zagoruiko.rates.service.SparkService;
import org.zagoruiko.rates.writer.DatasetWriter;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
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
    private SparkService sparkService;
    private SparkSession spark;

    private DatasetWriter ratesWriter;
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setSparkService(SparkService sparkService) {
        this.sparkService = sparkService;
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
        System.out.println(String.join(",", args));

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(Main.class.getPackage().getName());
        context.refresh();
        context.getBean(Main.class).run(args);
    }

    public void run(String[] args) {
        sparkService.initCurrenciesTables();
        sparkService.initInvestingTables();
        sparkService.repairCurrenciesTables();
        sparkService.repairInvestingTables();


        Dataset<Row> united = sparkService.processCurrencies();
        ratesWriter.write(united, "expenses.rates");

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