package org.zagoruiko.rates.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value = "classpath:application.properties")
public class S3Config {
    @Value("${s3.endpoint}")
    private String s3Endpoint;

    @Value("${s3.access.key}")
    private String s3AccessKey;

    @Value("${s3.secret.key}")
    private String s3SecretKey;

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }


}
