package org.dromara.northstar.gateway.binance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.dromara.northstar.common.model.core.ContractDefinition;
import org.dromara.northstar.common.model.core.TimeSlot;
import org.dromara.northstar.common.model.core.TradeTimeDefinition;
import org.dromara.northstar.common.utils.DateTimeUtils;
import org.dromara.northstar.gateway.IMarketCenter;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.enums.DefaultUrls;
import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.impl.UMFuturesClientImpl;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class BinanceContractProvider {

    private IMarketCenter mktCenter;

    private BinanceDataServiceManager dataMgr;

    UMFuturesClientImpl client;

    private TimeSlot allDay = TimeSlot.builder().start(DateTimeUtils.fromCacheTime(0, 0)).end(DateTimeUtils.fromCacheTime(0, 0)).build();


    public BinanceContractProvider(BinanceGatewaySettings settings, IMarketCenter mktCenter, BinanceDataServiceManager dataMgr) {
        this.mktCenter = mktCenter;
        this.dataMgr = dataMgr;
        this.client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey(), settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);
    }

    public void loadContractOptions() {

        try {

            //查询持仓模式
            String positionMode = client.account().getCurrentPositionMode(new LinkedHashMap<>());
            JSONObject positionModeJson = JSON.parseObject(positionMode);

            Boolean dualSidePosition = positionModeJson.getBoolean("dualSidePosition");
            //单向持仓模式需要修改为双向持仓模式
            if (!dualSidePosition) {
                LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
                parameters.put("dualSidePosition", "true");
                client.account().changePositionModeTrade(parameters);
            }

            //交易规则和交易对
            String result = client.market().exchangeInfo();
            //账户信息V2
            String account = client.account().accountInformation(new LinkedHashMap<>());

            JSONObject json = JSON.parseObject(result);
            JSONArray symbols = json.getJSONArray("symbols");

            //更新合约多头空头保证金率
            JSONObject jsonObject = JSON.parseObject(account);
            JSONArray positions = jsonObject.getJSONArray("positions");

            //多头Map
            Map<String, JSONObject> longPositionMap = positions.stream()
                    .map(item -> (JSONObject) item)
                    .filter(item -> "LONG".equals(item.getString("positionSide")))
                    .collect(Collectors.toMap(x -> x.getString("symbol"), x -> x, (oldValue, newValue) -> oldValue));
            //空头Map
            Map<String, JSONObject> shortPositionMap = positions.stream()
                    .map(item -> (JSONObject) item)
                    .filter(item -> "SHORT".equals(item.getString("positionSide")))
                    .collect(Collectors.toMap(x -> x.getString("symbol"), x -> x, (oldValue, newValue) -> oldValue));

            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                JSONObject longSymbol = longPositionMap.get(obj.getString("symbol"));
                JSONObject shortSymbol = shortPositionMap.get(obj.getString("symbol"));
                //计算多头空头保证金率
                obj.put("longMarginRatio", 1 / longSymbol.getDoubleValue("leverage"));
                obj.put("shortMarginRatio", 1 / shortSymbol.getDoubleValue("leverage"));
                mktCenter.addInstrument(new BinanceContract(obj, dataMgr));
            }
        } catch (BinanceConnectorException e) {
            log.error("fullErrMessage: {}", e.getMessage(), e);
        } catch (BinanceClientException e) {
            log.error("fullErrMessage: {} \nerrMessage: {} \nerrCode: {} \nHTTPStatusCode: {}",
                    e.getMessage(), e.getErrMsg(), e.getErrorCode(), e.getHttpStatusCode(), e);
        }
    }

    public List<ContractDefinition> get() {
        List<ContractDefinition> contractDefs = new ArrayList<>();

        try {
            String result = client.market().exchangeInfo();
            JSONObject json = JSON.parseObject(result);
            JSONArray symbols = json.getJSONArray("symbols");
            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                BinanceContract contract = new BinanceContract(obj, dataMgr);
                ContractDefinition cnFtTt1 = ContractDefinition.builder()
                        .name(contract.name())
                        .exchange(contract.exchange())
                        .productClass(contract.productClass())
                        .symbolPattern(Pattern.compile(contract.name() + "@[A-Z]+@[A-Z]+@[A-Z]+$"))
                        .commissionRate(3 / 10000D).dataSource(dataMgr)
                        .tradeTimeDef(TradeTimeDefinition.builder().timeSlots(List.of(allDay)).build())
                        .build();
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
