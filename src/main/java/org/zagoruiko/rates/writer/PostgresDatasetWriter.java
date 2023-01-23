package org.zagoruiko.rates.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;

@Component("pgJdbcWriter")
public class PostgresDatasetWriter implements DatasetWriter {
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

    private String jdbcTable = "expenses.rates";

    @Override
    public void write(Dataset<Row> dataset, String jdbcTable) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("driver", this.jdbcDriver);
        jdbcProperties.setProperty("user", this.jdbcUser);
        jdbcProperties.setProperty("password", this.jdbcPassword);
        jdbcProperties.setProperty("truncate", "true");
        jdbcProperties.setProperty("batchsize", "10000");
        jdbcProperties.setProperty("isolationLevel", "NONE");
        dataset.select(
                col("asset"),
                col("quote"),
                col("date"),
                col("rate")
        )
        .write()
        .mode(SaveMode.Overwrite)
        .jdbc(this.jdbcUrl, this.jdbcTable, jdbcProperties);

    }
}
