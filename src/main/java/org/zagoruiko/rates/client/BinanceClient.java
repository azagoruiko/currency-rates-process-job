package org.zagoruiko.rates.client;

import org.zagoruiko.rates.client.dto.ExchangeInfoDTO;

public interface BinanceClient {
    ExchangeInfoDTO getExchangeInfo();
}
