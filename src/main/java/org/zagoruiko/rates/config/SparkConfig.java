package org.zagoruiko.rates.config;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Configuration
public class SparkConfig {
    private S3Config s3Config;

    @Autowired
    public SparkConfig(S3Config s3Config) {
        this.s3Config = s3Config;
    }

    @Bean
    public SparkSession sparkSession() {

        SparkSession.Builder builder = SparkSession.builder();
        if (System.getenv("LOCAL_MODE") != null) {
                    builder.master("local[*]").enableHiveSupport();
        }
        SparkSession spark = builder
                .appName("Rates_Update")
                .config("spark.driver.userClassPathFirst", false)
                .getOrCreate();

        spark.conf().set("spark.executor.userClassPathFirst", "true");

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);
        if (System.getenv("LOCAL_MODE") != null) {
            jsc.addJar("local:/opt/spark/jars/gson-2.8.5.jar,/opt/apache-hive-3.1.3-bin/lib/hive*.jar,/opt/apache-hive-3.1.3-bin/lib/*sql*.jar,/opt/apache-hive-3.1.3-bin/lib/datanucleus*.jar,/opt/apache-hive-3.1.3-bin/lib/Hikari*.jar,/opt/apache-hive-3.1.3-bin/lib/javax*.jar");
        }

        org.apache.hadoop.conf.Configuration conf=jsc.hadoopConfiguration();

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.endpoint", this.s3Config.getS3Endpoint());
        conf.set("fs.s3a.access.key", this.s3Config.getS3AccessKey());
        conf.set("fs.s3a.secret.key", this.s3Config.getS3SecretKey());

        return spark;
    }
}
