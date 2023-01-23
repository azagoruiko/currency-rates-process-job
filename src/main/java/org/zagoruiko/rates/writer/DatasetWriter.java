package org.zagoruiko.rates.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DatasetWriter {

    void write(Dataset<Row> dataset, String table);
}
