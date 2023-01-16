package org.zagoruiko.rates;

import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.zagoruiko.rates.service.SparkService;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@PropertySource(value = "classpath:application.properties")
public class Main {

    private static Format format = new SimpleDateFormat("yyyy-MM-dd");
    private static Calendar calendar = Calendar.getInstance();

    private SparkService sparkService;

    @Autowired
    public void setSparkService(SparkService sparkService) {
        this.sparkService = sparkService;
    }

    public static void main(String[] args) throws IOException, ParseException {
        System.out.println(String.join(",", args));

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(Main.class.getPackage().getName());
        context.refresh();
        context.getBean(Main.class).run(args);
    }

    public void run(String[] args) {
        sparkService.initCurrenciesTables();
        sparkService.initInvestingTables();
        sparkService.repairCurrenciesTables();
        sparkService.repairInvestingTables();

        sparkService.selectRate().show(500);
        sparkService.selectInvestingRate().show(500);
        //sparkService.selectInvestingRateAll().show(500);
    }
}