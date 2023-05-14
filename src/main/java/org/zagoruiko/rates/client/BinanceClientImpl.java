package org.zagoruiko.rates.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;
import org.zagoruiko.rates.client.dto.ExchangeInfoDTO;

import java.io.IOException;

@Service
public class BinanceClientImpl implements BinanceClient {
    private CloseableHttpClient client;

    public BinanceClientImpl() {
        this.client = HttpClients.createDefault();
    }

    @Override
    public ExchangeInfoDTO getExchangeInfo() {
        HttpGet request = new HttpGet("https://api.binance.com/api/v3/exchangeInfo");
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                return om.readValue(EntityUtils.toString(entity), ExchangeInfoDTO.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
