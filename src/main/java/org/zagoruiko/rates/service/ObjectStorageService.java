package org.zagoruiko.rates.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.springframework.stereotype.Service;
import org.zagoruiko.rates.util.Binance;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Service
public class ObjectStorageService implements StorageService {

    private static Format format = new SimpleDateFormat("yyyy-MM-dd");

    private AmazonS3 s3Client;

    private String convertTime(long time){
        Date date = new Date(time);
        return format.format(date);
    }

    public ObjectStorageService(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void storeAsCsvFile(String bucket, String table, String asset, String quote, List<List<Object>> data) throws IOException {
        Map<String, List<String>> output = Binance.klines2CSVMap(data);
        for (Map.Entry<String, List<String>> entry : output.entrySet()) {
            File file = new File("." + File.separator + entry.getKey() + ".csv");
            FileWriter fileWriter = new FileWriter(file, false);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(entry.getValue().stream().collect(Collectors.joining("\n")));
            /***
             * [
             *   [
             *     1499040000000,      // Kline open time
             *     "0.01634790",       // Open price
             *     "0.80000000",       // High price
             *     "0.01575800",       // Low price
             *     "0.01577100",       // Close price
             *     "148976.11427815",  // Volume
             *     1499644799999,      // Kline Close time
             *     "2434.19055334",    // Quote asset volume
             *     308,                // Number of trades
             *     "1756.87402397",    // Taker buy base asset volume
             *     "28.46694368",      // Taker buy quote asset volume
             *     "0"                 // Unused field, ignore.
             */
            printWriter.flush();
            printWriter.close();

            try {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,
                        String.format("%s/asset=%s/quote=%s/%s.csv", table, asset, quote, entry.getKey()),
                        file);

                this.s3Client.putObject(putObjectRequest);
                file.delete();
            } catch (Exception e) {
                Logger.getAnonymousLogger().log(Level.SEVERE, e.getMessage());
                System.exit(1);
            }
        }
    }

    @Override
    public void createPartition(String bucket, String table, String asset, String quote) throws IOException {
        File file = new File("." + File.separator + "__PARTITION__");
        file.createNewFile();
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,
                    String.format("%s/asset=%s/quote=%s/__PARTITION__", table, asset, quote),
                    file);

            this.s3Client.putObject(putObjectRequest);
            file.delete();
        } catch (Exception e) {
            Logger.getAnonymousLogger().log(Level.SEVERE, "Error saving dummy file for partition " + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void prepareTableFolder(String bucket, String table) throws IOException {
        File file = new File("." + File.separator + "__PARTITION__");
        file.createNewFile();
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,
                    String.format("%s/__PARTITION__", table),
                    file);

            this.s3Client.putObject(putObjectRequest);
            file.delete();
        } catch (Exception e) {
            Logger.getAnonymousLogger().log(Level.SEVERE, "Error saving dummy file " + e.getMessage());
            System.exit(1);
        }
    }
}
