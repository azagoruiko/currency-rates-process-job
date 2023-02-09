package org.zagoruiko.rates.client;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.stereotype.Service;
import org.zagoruiko.rates.client.dto.ExchangeInfoDTO;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

@Service
public class BinanceClientImpl implements BinanceClient {
    private ClientConfig cfg = new ClientConfig();
    private Client client;

    public BinanceClientImpl() {
        this.cfg.register(JacksonJsonProvider.class);
        this.client = ClientBuilder.newBuilder().withConfig(this.cfg).build();
    }

    @Override
    public ExchangeInfoDTO getExchangeInfo() {
        WebTarget target = client.target("https://api.binance.com/api/v3/exchangeInfo");
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return ib.get( ExchangeInfoDTO.class);
    }
}
