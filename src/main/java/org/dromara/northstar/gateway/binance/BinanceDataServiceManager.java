package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.enums.DefaultUrls;
import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.impl.UMFuturesClientImpl;

import org.dromara.northstar.common.IDataSource;
import org.dromara.northstar.common.ObjectManager;
import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.gateway.Gateway;
import org.dromara.northstar.gateway.model.ContractDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.ArrayList;
import java.util.Collections;
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
public class BinanceDataServiceManager implements IDataSource {

    @Autowired
    private BinanceGatewaySettings settings;
    @Autowired
    private ObjectManager<Gateway> gatewayManager;

    String format = LocalDate.now().format(DateTimeConstant.D_FORMAT_INT_FORMATTER);

    SimpleDateFormat sdf = new SimpleDateFormat("HHmmssSSS");


    private UMFuturesClientImpl client;

    @Override
    public List<CoreField.BarField> getMinutelyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        List<CoreField.BarField> allData = new ArrayList<>();

        LocalDateTime startTime = startDate.atStartOfDay();
        LocalDateTime endTime = endDate.atTime(LocalTime.MAX);

        long minutes = Duration.between(startTime, endTime).toMinutes();

        for (int i = 0; i <= minutes; i += 1000) {
            LocalDateTime currentStartTime = startTime.plusMinutes(i);
            LocalDateTime currentEndTime = currentStartTime.plusMinutes(999);

            Instant instantStart = currentStartTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant();
            Instant instantEnd = currentEndTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant();
            List<CoreField.BarField> data = getHistoricalData(contract, instantStart.toEpochMilli(), instantEnd.toEpochMilli(), "1m");
            allData.addAll(data);
        }
        return allData;
    }

    @Override
    public List<CoreField.BarField> getQuarterlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "15m");
    }

    @Override
    public List<CoreField.BarField> getHourlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "1h");
    }

    @Override
    public List<CoreField.BarField> getDailyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "1d");
    }

    @Override
    public List<LocalDate> getHolidays(CoreEnum.ExchangeEnum exchange, LocalDate startDate, LocalDate endDate) {
        return Collections.emptyList();
    }

    @Override
    public List<CoreField.ContractField> getAllContracts(CoreEnum.ExchangeEnum exchange) {
        LinkedList<CoreField.ContractField> resultList = new LinkedList<>();
        List<ContractDefinition> contractDefs = new ArrayList<>();
//        Gateway gateway = gatewayManager.get(Identifier.of(ChannelType.BIAN.toString()));
//        settings = (BinanceGatewaySettings) gateway.gatewayDescription().getSettings();
//        client = new UMFuturesClientImpl(settings.getApiKey(),settings.getSecretKey(), settings.isAccountType() ?  DefaultUrls.USDM_PROD_URL : DefaultUrls.TESTNET_URL);
        client = new UMFuturesClientImpl(settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);

        try {
            String result = client.market().exchangeInfo();
            JSONObject json = JSON.parseObject(result);
            JSONArray symbols = json.getJSONArray("symbols");
            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                BinanceContract contract = new BinanceContract(obj, this);
                ContractDefinition cnFtTt1 = ContractDefinition.builder().name(contract.name()).exchange(contract.exchange()).productClass(contract.productClass())
                        .symbolPattern(Pattern.compile("^[A-Z]+@[A-Z]+@[A-Z]+@[A-Z]+$")).tradeTimeType("CN_FT_TT1").commissionRate(3 / 10000D).build();
                contractDefs.add(cnFtTt1);
                resultList.add(contract.contractField());
            }
        } catch (BinanceConnectorException e) {
            log.error("fullErrMessage: {}", e.getMessage(), e);
        } catch (BinanceClientException e) {
            log.error("fullErrMessage: {} \nerrMessage: {} \nerrCode: {} \nHTTPStatusCode: {}",
                    e.getMessage(), e.getErrMsg(), e.getErrorCode(), e.getHttpStatusCode(), e);
        }
        return resultList;
    }

    @Override
    public List<CoreEnum.ExchangeEnum> getUserAvailableExchanges() {
        return Collections.singletonList(CoreEnum.ExchangeEnum.BINANCE);
    }

    public List<CoreField.BarField> getHistoricalData(CoreField.ContractField contract, long startDate, long endDate, String interval) {
        log.debug("历史行情{}数据：{}，{} -> {}", interval, contract.getUnifiedSymbol(), startDate, endDate);
        Gateway gateway = gatewayManager.get(Identifier.of(ChannelType.BIAN.toString()));
        settings = (BinanceGatewaySettings) gateway.gatewayDescription().getSettings();
        client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey(), settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);

        LocalDateTime dateTime = null;
        String actionTime = "";

        LinkedList<CoreField.BarField> barFieldList = new LinkedList<>();
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", contract.getSymbol());
        parameters.put("interval", interval);
        parameters.put("startTime", startDate);
        parameters.put("endTime", endDate);
        parameters.put("limit", 1500);
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
