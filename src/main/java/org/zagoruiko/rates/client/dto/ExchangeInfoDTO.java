package org.zagoruiko.rates.client.dto;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ExchangeInfoDTO {
    private List<SymbolDTO> symbols;

    public ExchangeInfoDTO() {
    }

    public List<SymbolDTO> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<SymbolDTO> symbols) {
        this.symbols = symbols;
    }
}
