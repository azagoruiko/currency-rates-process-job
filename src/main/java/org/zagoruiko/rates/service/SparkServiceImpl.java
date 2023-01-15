package org.zagoruiko.rates.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.util.List;

@Service
public class SparkServiceImpl implements SparkService {
    private SparkSession spark;
    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.spark = sparkSession;
    }
    @Override
    public void initCurrenciesTables() {
        //spark.sql("DROP TABLE currencies");

        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS binance_currencies " +
                "(Timestamp DATE,High FLOAT,Low FLOAT,Open FLOAT,Close FLOAT) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                "PARTITIONED BY (asset STRING, quote STRING) " +
                "LOCATION 's3a://currency/binance/' " +
                "tblproperties (\"skip.header.line.count\"=\"1\")");

        repairCurrenciesTables();
    }

    @Override
    public void initInvestingTables() {
        spark.sql("DROP TABLE IF EXISTS investing_currencies");
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
    public void repairCurrenciesTables() {
        spark.sql("MSCK REPAIR TABLE binance_currencies").select().show();
    }

    @Override
    public void repairInvestingTables() {
        spark.sql("MSCK REPAIR TABLE investing_currencies").select().show();
    }


    @Override
    public Dataset<Row> selectRate() {
        return spark.sql("SELECT asset, quote, date_trunc(to_date(timestamp, 'yyyy-MM-dd'), 'YEAR') year, max(timestamp) max_ts, min(Timestamp) min_ts, min(Close) min_close, max(Close) max_close " +
                "FROM binance_currencies group by asset, quote, date_trunc(to_date(timestamp, 'yyyy-MM-dd'), 'YEAR')").select(
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
        return spark.sql("SELECT asset, quote, date_trunc(to_date(Date, 'MM/dd/yyyy'), 'YEAR') year, max(Date) max_ts, min(Date) min_ts, min(Price) min_close, max(Price) max_close " +
                "FROM investing_currencies group by asset, quote, date_trunc(to_date(Date, 'MM/dd/yyyy'), 'YEAR')").select(
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
        return spark.sql("SELECT *, to_date(Date, 'MM/dd/yyyy') the_date, date_trunc('YEAR', to_date(Date, 'MM/dd/yyyy')) year " +
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
