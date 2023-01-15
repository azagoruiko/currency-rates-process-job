package org.zagoruiko.rates.client;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public interface RatesClient {
    List<List<Object>> loadContents(String asset, String quote, Date from, int limit) throws IOException;
}
