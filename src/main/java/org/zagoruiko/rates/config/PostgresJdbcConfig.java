package org.zagoruiko.rates.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class PostgresJdbcConfig {
    @Value("${postgres.jdbc.url}")
    String jdbcUrl;

    @Value("${postgres.jdbc.driver}")
    String jdbcDriver;

    @Value("${postgres.jdbc.user}")
    String jdbcUser;

    @Value("${postgres.jdbc.password}")
    String jdbcPassword;

    @Bean(name = "postgres.jdbc.url")
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Bean(name = "postgres.jdbc.driver")
    public String getJdbcDriver() {
        return jdbcDriver;
    }

    @Bean(name = "postgres.jdbc.user")
    public String getJdbcUser() {
        return jdbcUser;
    }

    @Bean(name = "postgres.jdbc.password")
    public String getJdbcPassword() {
        return jdbcPassword;
    }

    @Bean
    public DataSource getDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(this.jdbcDriver);
        dataSource.setUrl(this.jdbcUrl);
        dataSource.setUsername(this.jdbcUser);
        dataSource.setPassword(this.jdbcPassword);

        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(this.getDataSource());
    }
}
