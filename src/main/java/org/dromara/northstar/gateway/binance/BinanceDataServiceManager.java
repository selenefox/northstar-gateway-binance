package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.impl.UMFuturesClientImpl;

import org.dromara.northstar.common.IDataServiceManager;
import org.dromara.northstar.common.ObjectManager;
import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.gateway.Gateway;
import org.dromara.northstar.gateway.model.ContractDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreEnum;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.BarField;

/**
 * @author 李嘉豪
 * @version 1.0
 * @date 2023/9/20 17:38
 */
@Slf4j
public class BinanceDataServiceManager implements IDataServiceManager {

    @Autowired
    private BinanceGatewaySettings settings;
    @Autowired
    private ObjectManager<Gateway> gatewayManager;

    String format = LocalDate.now().format(DateTimeConstant.D_FORMAT_INT_FORMATTER);

    private UMFuturesClientImpl client = new UMFuturesClientImpl();

    @Override
    public List<CoreField.BarField> getMinutelyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract,startDate,endDate,"1m");
    }

    @Override
    public List<CoreField.BarField> getQuarterlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract,startDate,endDate,"15m");
    }

    @Override
    public List<CoreField.BarField> getHourlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract,startDate,endDate,"1h");
    }

    @Override
    public List<CoreField.BarField> getDailyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract,startDate,endDate,"1d");
    }

    @Override
    public List<LocalDate> getHolidays(CoreEnum.ExchangeEnum exchange, LocalDate startDate, LocalDate endDate) {
        return null;
    }

    @Override
    public List<CoreField.ContractField> getAllContracts(CoreEnum.ExchangeEnum exchange) {
        UMFuturesClientImpl client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey());
        LinkedList<CoreField.ContractField> resultList = new LinkedList<>();
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
        return resultList;
    }

    public List<CoreField.BarField> getHistoricalData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate, String interval) {
        log.debug("历史行情{}数据：{}，{} -> {}", interval, contract.getUnifiedSymbol(), startDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER), endDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER));
        Gateway gateway = gatewayManager.get(Identifier.of(ChannelType.BIAN.toString()));
        settings = (BinanceGatewaySettings) gateway.gatewayDescription().getSettings();

        LocalDateTime dateTime = null;
        String actionTime = "";

        LinkedList<CoreField.BarField> barFieldList = new LinkedList<>();
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", contract.getSymbol());
        parameters.put("interval", interval);
        parameters.put("startTime", startDate);
        parameters.put("endTime", endDate);
        String result = client.market().klines(parameters);
        List<String[]> klinesList = JSON.parseArray(result, String[].class);
        double quantityPrecision = 1 / Math.pow(10, contract.getQuantityPrecision());

        for (String[] s : klinesList) {
            // 将时间戳转换为LocalDateTime对象
            dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(s[0])), ZoneId.systemDefault());
            actionTime = dateTime.format(DateTimeConstant.T_FORMAT_WITH_MS_INT_FORMATTER);

            double volume = Double.parseDouble(s[5]) / quantityPrecision;
            double turnover = Double.parseDouble(s[7]);
            double numTrades = Double.parseDouble(s[8]);
            barFieldList.addFirst(BarField.newBuilder()
                    .setUnifiedSymbol(contract.getSymbol())
                    .setGatewayId(contract.getGatewayId())
                    .setTradingDay(format)
                    .setActionDay(format)
                    .setActionTime(actionTime)
                    .setActionTimestamp(Long.parseLong(s[0]))
                    .setOpenPrice(Double.valueOf(s[1]))
                    .setHighPrice(Double.valueOf(s[2]))
                    .setLowPrice(Double.valueOf(s[3]))
                    .setClosePrice(Double.valueOf(s[4]))
                    .setVolume((long) volume)
                    .setVolumeDelta((long) volume)
                    .setTurnover(turnover)
                    .setTurnoverDelta(turnover)
                    .setNumTrades((long) numTrades)
                    .setNumTradesDelta((long) numTrades)
                    .setChannelType(ChannelType.BIAN.toString())
                    .build());
        }
        return barFieldList;
    }
}
