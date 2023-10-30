package org.dromara.northstar.gateway.binance;

import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.model.ContractDefinition;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.impl.UMFuturesClientImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class BinanceContractProvider {

    private IMarketCenter mktCenter;

    private BinanceGatewaySettings settings;

    public BinanceContractProvider(BinanceGatewaySettings settings, IMarketCenter mktCenter) {
        this.mktCenter = mktCenter;
        this.settings = settings;
    }

    public void loadContractOptions() {
        UMFuturesClientImpl client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey());
        try {
            String result = client.market().exchangeInfo();
            JSONObject json = JSON.parseObject(result);
            JSONArray symbols = json.getJSONArray("symbols");
            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                mktCenter.addInstrument(new BinanceContract(obj));
            }
        } catch (BinanceConnectorException e) {
            log.error("fullErrMessage: {}", e.getMessage(), e);
        } catch (BinanceClientException e) {
            log.error("fullErrMessage: {} \nerrMessage: {} \nerrCode: {} \nHTTPStatusCode: {}",
                    e.getMessage(), e.getErrMsg(), e.getErrorCode(), e.getHttpStatusCode(), e);
        }
    }

    public List<ContractDefinition> get() {
        UMFuturesClientImpl client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey());
        List<ContractDefinition> contractDefs = new ArrayList<>();
        try {
            String result = client.market().exchangeInfo();
            JSONObject json = JSON.parseObject(result);
            JSONArray symbols = json.getJSONArray("symbols");
            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                BinanceContract contract = new BinanceContract(obj);
                ContractDefinition cnFtTt1 = ContractDefinition.builder().name(contract.name()).exchange(contract.exchange()).productClass(contract.productClass())
                        .symbolPattern(Pattern.compile("^[A-Z]+@[A-Z]+@[A-Z]+@[A-Z]+$")).tradeTimeType("CN_FT_TT1").commissionRate(3 / 10000D).build();
                contractDefs.add(cnFtTt1);
            }
        } catch (BinanceConnectorException e) {
            log.error("fullErrMessage: {}", e.getMessage(), e);
        } catch (BinanceClientException e) {
            log.error("fullErrMessage: {} \nerrMessage: {} \nerrCode: {} \nHTTPStatusCode: {}",
                    e.getMessage(), e.getErrMsg(), e.getErrorCode(), e.getHttpStatusCode(), e);
        }
        return contractDefs;
    }
}
