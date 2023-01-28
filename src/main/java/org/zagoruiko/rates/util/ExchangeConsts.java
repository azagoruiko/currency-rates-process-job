package org.zagoruiko.rates.util;

public enum ExchangeConsts {
    //Date(UTC)	Pair	Side	Price	Executed	Amount	Fee
    BINANCE("BUY", "SELL", "Side", "Price", "Amount", "Executed", "Pair", "Date(UTC)", "yyyy-MM-dd HH:mm:ss"),
//    Date/Time	Trade ID	Type	Currency Pair	Amount	Price	Total	Fee type	Fee amount	Fee %

    EXMO("buy", "sell", "Type", "Price", "Total", "Amount", "Currency Pair", "Date/Time", "dd.MM.yyyy HH:mm")
    ;
    private String buyStr;
    private String sellStr;
    private String sideCol;
    private String priceCol;
    private String amountCol;
    private String totalCol;

    private String pairCol;
    private String dateCol;
    private String dateFormat;

    ExchangeConsts(String buyStr, String sellStr, String sideCol, String priceCol, String amountCol, String totalCol, String pairCol, String dateCol, String dateFormat) {
        this.buyStr = buyStr;
        this.sellStr = sellStr;
        this.sideCol = sideCol;
        this.priceCol = priceCol;
        this.amountCol = amountCol;
        this.totalCol = totalCol;
        this.pairCol = pairCol;
        this.dateCol = dateCol;
        this.dateFormat = dateFormat;
    }

    public String getBuyStr() {
        return buyStr;
    }

    public String getSellStr() {
        return sellStr;
    }

    public String getSideCol() {
        return sideCol;
    }

    public String getPriceCol() {
        return priceCol;
    }

    public String getAmountCol() {
        return amountCol;
    }

    public String getTotalCol() {
        return totalCol;
    }

    public String getPairCol() {
        return pairCol;
    }

    public String getDateCol() {
        return dateCol;
    }

    public String getDateFormat() {
        return dateFormat;
    }
}
