package org.zagoruiko.rates.service;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

@Service
public class RatesSparkServiceImpl implements RatesSparkService {

    private SparkSession spark;
    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    @Override
    public void repairHiveTable(String tableName) {
        spark.sql(String.format("MSCK REPAIR TABLE %s", tableName)).select().show();
    }

    @Override
    public void initCurrenciesTables() {
        //spark.sql("DROP TABLE currencies");
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS currencylayer " +
                "(date DATE,ast FLOAT,qout FLOAT,rate FLOAT) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                "PARTITIONED BY (asset STRING, quote STRING) " +
                "LOCATION 's3a://currency/currencylayer/' " +
                "");

        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS binance_currencies " +
                "(Timestamp DATE,High FLOAT,Low FLOAT,Open FLOAT,Close FLOAT) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                "PARTITIONED BY (asset STRING, quote STRING) " +
                "LOCATION 's3a://currency/exchange=binance/' " +
                "tblproperties (\"skip.header.line.count\"=\"1\")");

        repairCurrenciesTables();
    }

    @Override
    public void initCryptoRates() {
        //spark.sql("DROP TABLE currencies");

        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS crypto_rates " +
                "(Timestamp DATE,High FLOAT,Low FLOAT,Open FLOAT,Close FLOAT) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                "PARTITIONED BY (exchange STRING, asset STRING, quote STRING) " +
                "LOCATION 's3a://currency/' " +
                "tblproperties (\"skip.header.line.count\"=\"1\")");

        repairCryptoRatesTables();
    }

    @Override
    public void initInvestingTables() {
        //spark.sql("DROP TABLE IF EXISTS investing_currencies");
        //"Date","Price","Open","High","Low","Vol.","Change %"
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS investing_currencies " +
                "(Date STRING,Price FLOAT,Open FLOAT,High FLOAT,Low FLOAT,Vol STRING,Change STRING) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\"' " +
                "PARTITIONED BY (asset STRING, quote STRING) " +
                "LOCATION 's3a://investing.com.rates/main/' " +
                "tblproperties (\"skip.header.line.count\"=\"1\")");

        repairInvestingTables();
    }

    @Override
    public void repairCryptoRatesTables() {
        repairHiveTable("crypto_rates");
    }

    @Override
    public void repairCurrenciesTables() {
        repairHiveTable("binance_currencies");
        repairHiveTable("currencylayer");
    }

    @Override
    public void repairInvestingTables() {
        repairHiveTable("investing_currencies");
    }


    @Override
    public Dataset<Row> selectRate() {
        return spark.sql("SELECT asset, quote, year(to_date(timestamp, 'yyyy-MM-dd')) year, max(to_date(timestamp, 'yyyy-MM-dd')) max_ts, " +
                "min(to_date(timestamp, 'yyyy-MM-dd')) min_ts, min(Close) min_close, max(Close) max_close " +
                "FROM binance_currencies group by asset, quote, year(to_date(timestamp, 'yyyy-MM-dd'))").select(
                functions.col("Asset"),
                functions.col("Quote"),
                functions.col("year"),
                functions.col("min_ts"),
                functions.col("max_ts"),
                functions.col("min_close"),
                functions.col("max_close")
        );
    }

    @Override
    public Dataset<Row> selectInvestingRate() {
        return spark.sql("SELECT asset, quote, year(to_date(Date, 'MM/dd/yyyy')) year, max(to_date(Date, 'MM/dd/yyyy')) max_ts, min(to_date(Date, 'MM/dd/yyyy')) min_ts, min(Price) min_close, max(Price) max_close " +
                "FROM investing_currencies group by asset, quote, year(to_date(Date, 'MM/dd/yyyy'))").select(
                functions.col("Asset"),
                functions.col("Quote"),
                functions.col("year"),
                functions.col("min_ts"),
                functions.col("max_ts"),
                functions.col("min_close"),
                functions.col("max_close")
        );
    }

    @Override
    public Dataset<Row> selectInvestingRateAll() {
        return spark.sql("SELECT *, to_date(Date, 'MM/dd/yyyy') the_date, year(to_date(Date, 'MM/dd/yyyy')) year " +
                "FROM investing_currencies").select(
                functions.col("Asset"),
                functions.col("Quote"),
                functions.col("year"),
                functions.col("the_date"),
                functions.col("Price"),
                functions.col("Date"),
                functions.col("High"),
                functions.col("Low")
        );
    }

    public SparkSession getSpark() {
        return spark;
    }

    @Override
    public Dataset<Row> selectInvestingOverUSDTRate() {
        Dataset<Row> old = spark.sql("SELECT COALESCE(ic.asset, (CASE WHEN bc.asset = 'USDT' THEN 'USD' ELSE bc.asset END)) asset, " +
                "COALESCE(ic.quote, (CASE WHEN bc.quote = 'USDT' THEN 'USD' ELSE bc.quote END)) quote,  " +
                "COALESCE(to_date(bc.timestamp, 'yyyy-MM-dd'), to_date(ic.Date, 'MM/dd/yyyy')) date, " +
                "COALESCE(COALESCE((CASE " +
                "   WHEN (ic.quote='UAH' AND ic.asset='USD' AND bc.Close is NULL) " +
                "   OR (ic.quote='BTC' AND ic.asset='USD' AND bc.Close is NULL) " +
                "   OR (ic.quote='USD' AND ic.asset='EUR' AND bc.Close is NULL) " +
                "   THEN ic.Price " +
                "   ELSE bc.Close END), bc.Close), ic.Price) rate, " +
                "ic.Price ic_rate, " +
                "bc.Close bc_rate, " +
                "to_date(ic.Date, 'MM/dd/yyyy') ic_date " +
                "FROM binance_currencies bc " +
                "FULL JOIN investing_currencies ic " +
                "ON ic.asset = (CASE WHEN bc.asset = 'USDT' THEN 'USD' ELSE bc.asset END) " +
                "AND ic.quote = (CASE WHEN bc.quote = 'USDT' THEN 'USD' ELSE bc.quote END) " +
                "AND to_date(ic.Date, 'MM/dd/yyyy') = to_date(bc.timestamp, 'yyyy-MM-dd')").select(
                functions.col("asset"),
                functions.col("quote"),
                functions.col("date"),
                functions.col("rate")
        );
        return old; //old.union(spark.sql("SELECT asset, quote, date, rate FROM currencylayer"));
    }

    @Override
    public Dataset<Row> selectCryptoRates() {
        return spark.sql("SELECT * FROM crypto_rates")
                .withColumn("date", functions.to_date(functions.col("Timestamp"), "yyyy-MM-dd"))
                .select(
                        functions.col("asset"),
                        functions.col("quote"),
                        functions.col("date"),
                        functions.col("Close").as("rate")
                        //functions.col("exchange")
                );
    }

    @Override
    public Dataset processCurrencies(String ... currencies) {
        Dataset united = this.selectInvestingOverUSDTRate()
                .filter(functions.col("asset").isin(currencies).or(functions.col("quote").isin(currencies)));
        united.createOrReplaceTempView("mycurrencies");
        List<Dataset<Row>> datasetsToUnion = new ArrayList<>();
        class SwappingTriplet {
            String newAsset;
            String newQuote;
            String intermediateAsset;

            public SwappingTriplet(String newAsset, String newQuote, String intermediateAsset) {
                this.newAsset = newAsset;
                this.newQuote = newQuote;
                this.intermediateAsset = intermediateAsset;
            }
        }
        for (SwappingTriplet swapper : new SwappingTriplet[]{
                new SwappingTriplet("CZK", "UAH", "USD"),
        }) {
            Dataset swapped = spark.sql("SELECT src.quote, trg.quote, src.date, trg.rate / src.rate as czk_uah " +
                    "FROM mycurrencies src " +
                    "JOIN mycurrencies trg " +
                    String.format("ON src.date = trg.date AND src.asset = '%s' and trg.asset = '%s' and src.quote = '%s' and trg.quote = '%s'",
                            swapper.intermediateAsset, swapper.intermediateAsset, swapper.newAsset, swapper.newQuote));
            united = united.union(swapped);
        }

        united.createOrReplaceTempView("mycurrencies_swapped");

        for (SwappingTriplet swapper : new SwappingTriplet[]{
                new SwappingTriplet("SOL", "UAH", "USD"),
                //new SwappingTriplet("BTC", "UAH", "USD"),
                new SwappingTriplet("BTC", "CZK", "USD"),
        }) {
            Dataset swapped = spark.sql("SELECT src.asset, trg.quote, src.date, trg.rate * src.rate as czk_uah " +
                    "FROM mycurrencies_swapped src " +
                    "JOIN mycurrencies_swapped trg " +
                    String.format("ON src.date = trg.date AND src.asset = '%s' and trg.asset = '%s' and src.quote = '%s' and trg.quote = '%s'",
                            swapper.newAsset, swapper.intermediateAsset, swapper.intermediateAsset, swapper.newQuote));
            united = united.union(swapped);
        }

        united.createOrReplaceTempView("mycurrencies_united");
        for (String[] pair : new String[][]{
                new String[]{"CZK", "UAH"},
                new String[]{"USD", "UAH"},
                new String[]{"USD", "CZK"},
                new String[]{"BTC", "USD"},
                new String[]{"BTC", "UAH"},
                new String[]{"BTC", "EUR"},
                new String[]{"BTC", "CZK"},
                new String[]{"EUR", "CZK"},
                new String[]{"EUR", "UAH"},
        }) {
            Dataset swapped = spark.sql("SELECT src.quote, src.asset, src.date, 1 / src.rate as rate " +
                    "FROM mycurrencies_united src " +
                    String.format("WHERE src.asset = '%s' and src.quote = '%s'",
                            pair[0], pair[1]));
            united = united.union(swapped);
        }

        Dataset one2one  = united.select(
                functions.col("asset"),
                functions.col("asset"),
                functions.col("date"),
                functions.lit(1f)
        ).dropDuplicates();
        united = united.union(one2one);

        united.createOrReplaceTempView("mycurrencies2");
        Dataset<Row> dates = this.spark.sql("SELECT asset, quote, max(date) max_date, min(date) min_date " +
                        "FROM mycurrencies2 " +
                        "GROUP BY asset, quote ")
                .withColumn("date", functions.explode(functions.expr("sequence(min_date, max_date, interval 1 day)")));

        dates.createOrReplaceTempView("dates");

        Dataset enriched = spark.sql("SELECT dat.asset, dat.quote, cur.date, dat.date, coalesce(cur.date, dat.date) the_date, cur.rate " +
                "FROM dates dat " +
                "LEFT JOIN mycurrencies2 cur " +
                "ON dat.date=cur.date AND dat.asset=cur.asset AND dat.quote=cur.quote")
                .repartition(
                    functions.col("asset"),
                    functions.col("quote")
                );

        WindowSpec window = Window
                .partitionBy("dat.asset", "dat.quote")
                .orderBy("dat.asset", "dat.quote", "dat.date");

        enriched = enriched.withColumn("last_rate", functions.coalesce(
                functions.col("cur.rate"),
                functions.lag("cur.rate", 1).over(window),
                functions.lag("cur.rate", 2).over(window),
                functions.lag("cur.rate", 3).over(window)
                ))
                .filter(functions.col("cur.date").isNull())
                .orderBy(functions.col("dat.asset"),
                        functions.col("dat.quote"),
                        functions.col("dat.date"))
                .select(functions.col("asset"),
                        functions.col("quote"),
                        functions.col("the_date"),
                        functions.col("last_rate")
                );


        united = united.union(enriched)
                .dropDuplicates()
                .repartition(
                functions.col("asset"),
                functions.col("quote")
        );

        return united;
    }

    @Override
    public Dataset processExchangeCurrencies() {
        Dataset united = this.selectCryptoRates();

        united.createOrReplaceTempView("mycurrencies2");
        Dataset<Row> dates = this.spark.sql("SELECT asset, quote, max(date) max_date, min(date) min_date " +
                        "FROM mycurrencies2 " +
                        "GROUP BY asset, quote ")
                .withColumn("date", functions.explode(functions.expr("sequence(min_date, max_date, interval 1 day)")));

        dates.createOrReplaceTempView("dates");

        Dataset enriched = spark.sql("SELECT dat.asset, dat.quote, cur.date, dat.date, coalesce(cur.date, dat.date) the_date, cur.rate " +
                        "FROM dates dat " +
                        "LEFT JOIN mycurrencies2 cur " +
                        "ON dat.date=cur.date AND dat.asset=cur.asset AND dat.quote=cur.quote")
                .repartition(
                        functions.col("asset"),
                        functions.col("quote")
                );

        WindowSpec window = Window
                .partitionBy("dat.asset", "dat.quote")
                .orderBy("dat.asset", "dat.quote", "dat.date");

        enriched = enriched.withColumn("last_rate", functions.coalesce(
                        functions.col("cur.rate"),
                        functions.lag("cur.rate", 1).over(window),
                        functions.lag("cur.rate", 2).over(window),
                        functions.lag("cur.rate", 3).over(window)
                ))
                .filter(functions.col("cur.date").isNull())
                .orderBy(functions.col("dat.asset"),
                        functions.col("dat.quote"),
                        functions.col("dat.date"))
                .select(functions.col("asset"),
                        functions.col("quote"),
                        functions.col("the_date"),
                        functions.col("last_rate")
                );


        united = united.union(enriched)
                .dropDuplicates()
                .repartition(
                        functions.col("asset"),
                        functions.col("quote")
                );

        return united;
    }

    @Override
    public Dataset processCurrencyLayer() {
        Dataset<Row> source = spark.sql("SELECT date, asset, quote, rate, 1 as prio from currencylayer");
        Dataset<Row> sameAsset = spark.sql("SELECT date, asset, asset as quote, 1.0 as rate, 2 as prio from currencylayer");
        Dataset<Row> sameQuote = spark.sql("SELECT date, quote as asset, quote, 1.0 as rate, 3 as prio from currencylayer");
        Dataset<Row> swapped = spark.sql("SELECT date, quote as asset, asset as quote, 1.0 / rate as rate, 2 as prio from currencylayer");

        Dataset<Row> unioned = source.unionAll(sameAsset).unionAll(sameQuote).unionAll(swapped);
        WindowSpec window = Window.partitionBy("date", "asset", "quote").orderBy("prio");
        return unioned.withColumn("rank", functions.row_number().over(window)).filter(functions.col("rank").equalTo(1)).drop("rank");
    }

    @Override
    public Date selectMaxDate(String source, String asset, String quote) {
        List<Row> output = spark.sql(String.format("SELECT COALESCE(MAX(Timestamp), to_date('2015-01-01')) max_date FROM binance_currencies WHERE" +
                " asset='%s' AND quote='%s'", asset, quote))
                .select(functions.col("max_date")).collectAsList();
        if (output.size() > 0 && output.get(0).size() > 0) {
            System.out.format("!!!! %s - %s", output.get(0), output.get(0).getAs(0));
            return output.get(0).getDate(0);
        } else {
            return new Date(0);
        }
    }


}
