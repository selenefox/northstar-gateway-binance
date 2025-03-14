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
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.common.model.core.Bar;
import org.dromara.northstar.common.model.core.Contract;
import org.dromara.northstar.gateway.Gateway;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

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

    private UMFuturesClientImpl client;

    @Override
    public List<Bar> getMinutelyData(Contract contract, LocalDate startDate, LocalDate endDate) {
        List<Bar> allData = new ArrayList<>();

        LocalDateTime startTime = startDate.atStartOfDay();
        LocalDateTime endTime = endDate.atTime(LocalTime.MAX);

        long minutes = Duration.between(startTime, endTime).toMinutes();

        //按照1000分钟进行分割
        for (int i = 0; i <= minutes; i += 1000) {
            LocalDateTime currentStartTime = startTime.plusMinutes(i);
            LocalDateTime currentEndTime;
            long l = minutes - i;
            //最后不满1000分钟则取尾数
            if (l < 1000) {
                currentEndTime = currentStartTime.plusMinutes(l);
            } else {
                currentEndTime = currentStartTime.plusMinutes(999);
            }

            Instant instantStart = currentStartTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant();
            Instant instantEnd = currentEndTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant();
            List<Bar> data = getHistoricalData(contract, instantStart.toEpochMilli(), instantEnd.toEpochMilli(), "1m");
            allData.addAll(data);
        }
        allData.sort(Comparator.comparing(Bar::actionTimestamp).reversed());
        return allData;
    }

    @Override
    public List<Bar> getQuarterlyData(Contract contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atTime(LocalTime.MAX).atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "15m");
    }

    @Override
    public List<Bar> getHourlyData(Contract contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atTime(LocalTime.MAX).atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "1h");
    }

    @Override
    public List<Bar> getDailyData(Contract contract, LocalDate startDate, LocalDate endDate) {
        return getHistoricalData(contract, startDate.atStartOfDay().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), endDate.atTime(LocalTime.MAX).atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli(), "1d");
    }

    @Override
    public List<LocalDate> getHolidays(ChannelType exchange, LocalDate startDate, LocalDate endDate) {
        return Collections.emptyList();
    }

    @Override
    public List<Contract> getAllContracts() {
        LinkedList<Contract> resultList = new LinkedList<>();
        client = new UMFuturesClientImpl(settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);

        try {
            String result = client.market().exchangeInfo();
            JSONObject json = JSON.parseObject(result).getJSONObject("data");
            JSONArray symbols = json.getJSONArray("symbols");
            for (int i = 0; i < symbols.size(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                BinanceContract contract = new BinanceContract(obj, this);
                resultList.add(contract.contract());
            }
        } catch (BinanceConnectorException e) {
            log.error("fullErrMessage: {}", e.getMessage(), e);
        } catch (BinanceClientException e) {
            log.error("fullErrMessage: {} \nerrMessage: {} \nerrCode: {} \nHTTPStatusCode: {}",
                    e.getMessage(), e.getErrMsg(), e.getErrorCode(), e.getHttpStatusCode(), e);
        }
        return resultList;
    }

    public List<Bar> getHistoricalData(Contract contract, long startDate, long endDate, String interval) {
        log.debug("历史行情{}数据：{}，{} -> {}", interval, contract.unifiedSymbol(), startDate, endDate);
        Gateway gateway = gatewayManager.get(Identifier.of(ChannelType.BIAN.toString()));
        if(gateway == null) {
        	throw new IllegalStateException("未有币安相关网关信息，请先创建一个币安网关");
        }
        settings = (BinanceGatewaySettings) gateway.gatewayDescription().getSettings();
        client = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey(), settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);
        LocalTime actionTime;
        LocalDate tradingDay;

        LinkedList<Bar> barFieldList = new LinkedList<>();
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("symbol", contract.symbol());
        parameters.put("interval", interval);
        parameters.put("startTime", startDate);
        parameters.put("endTime", endDate);
        parameters.put("limit", 1000);
        String result = client.market().klines(parameters);
        JSONObject jsonObject = JSON.parseObject(result);

        //当前权重值,1m不能超过2400
        /*int weight1m = jsonObject.getIntValue("x-mbx-used-weight-1m");
        log.info("币安API接口1m权重:[{}]", weight1m);
        if (weight1m > 2300) {
            try {
                log.info("币安API接口1m权重即将达到上限需线程等待1m:[{}]", weight1m);
                Thread.sleep(60000);
            } catch (Exception e) {
                log.error("币安API接口1m权重即将达到上限等待异常", e);
            }
        }*/
        String data = jsonObject.getJSONArray("data").toJSONString();
        List<String[]> klinesList = JSON.parseArray(data, String[].class);
        double quantityPrecision = 1 / Math.pow(10, contract.quantityPrecision());

        for (String[] s : klinesList) {
            // 将时间戳转换为LocalDateTime对象

            Instant e = Instant.ofEpochMilli(Long.parseLong(s[0]));
            actionTime = e.atZone(ZoneId.systemDefault()).toLocalTime();
            tradingDay = e.atZone(ZoneId.systemDefault()).toLocalDate();

            double volume = Double.parseDouble(s[5]) / quantityPrecision;
            double turnover = Double.parseDouble(s[7]);
            barFieldList.addFirst(Bar.builder()
                    .contract(contract)
                    .gatewayId(contract.gatewayId())
                    .tradingDay(tradingDay)
                    .actionDay(tradingDay)
                    .actionTime(actionTime)
                    .actionTimestamp(Long.parseLong(s[0]))
                    .openPrice(Double.valueOf(s[1]))
                    .highPrice(Double.valueOf(s[2]))
                    .lowPrice(Double.valueOf(s[3]))
                    .closePrice(Double.valueOf(s[4]))
                    .volume((long) volume)
                    .volumeDelta((long) volume)
                    .turnover(turnover)
                    .turnoverDelta(turnover)
                    .channelType(ChannelType.BIAN)
                    .build());
        }
        return barFieldList;
    }
}
