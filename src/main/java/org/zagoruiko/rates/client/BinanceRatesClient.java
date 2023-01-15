package org.zagoruiko.rates.client;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class BinanceRatesClient implements RatesClient {

    private RestTemplate restTemplate;

    public BinanceRatesClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public List<List<Object>> loadContents(String asset, String quote, Date from, int limit) throws IOException {
        String url = String.format("https://api1.binance.com/api/v3/klines?symbol=%s%s&interval=1d&limit=%s&startTime=%s",
                asset, quote, limit, from.getTime());
        return new ObjectMapper().readValue(restTemplate.getForObject(url, String.class), ArrayList.class);
    }
}
