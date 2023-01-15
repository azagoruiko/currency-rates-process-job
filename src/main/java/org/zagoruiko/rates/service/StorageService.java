package org.zagoruiko.rates.service;

import java.io.IOException;
import java.util.List;

public interface StorageService {
    void storeAsCsvFile(String bucket, String table, String asset, String quote, List<List<Object>> data) throws IOException;

    void createPartition(String bucket, String table, String asset, String quote) throws IOException;

    void prepareTableFolder(String bucket, String table) throws IOException;
}
