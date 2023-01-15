package org.zagoruiko.rates.util;

import com.amazonaws.services.s3.AmazonS3;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Binance {
    private static Format format = new SimpleDateFormat("yyyy-MM-dd");

    private static String convertTime(long time){
        Date date = new Date(time);
        return format.format(date);
    }
    public static Map<String, List<String>> klines2CSVMap(List<List<Object>> data) {
        List<String> rawList = data.stream().map(v ->
                        String.format("%s,%s,%s,%s,%s",
                                convertTime((Long)v.get(0)),
                                v.get(2), v.get(3), v.get(1), v.get(4)))
                .collect(Collectors.toList());
        //.collect(Collectors.joining("\n"));

        Map<String, List<String>> output = new HashMap<>();
        for (String line : rawList) {
            output.putIfAbsent(line.substring(0,7), new ArrayList<>());
            output.get(line.substring(0,7)).add(line);
        }
        return output;
    }
}
