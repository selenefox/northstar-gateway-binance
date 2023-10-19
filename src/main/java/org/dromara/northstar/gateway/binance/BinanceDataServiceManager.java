package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.impl.UMFuturesClientImpl;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.IDataServiceManager;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.gateway.model.ContractDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreEnum;
import xyz.redtorch.pb.CoreField;

/**
 * @author 李嘉豪
 * @version 1.0
 * @date 2023/9/20 17:38
 */
@Slf4j
public class BinanceDataServiceManager implements IDataServiceManager {

    @Autowired
    private BinanceGatewaySettings settings;

    @Override
    public List<CoreField.BarField> getMinutelyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        log.debug("历史行情1分钟数据：{}，{} -> {}", contract.getUnifiedSymbol(), startDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER), endDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER));
        return null;
    }

    @Override
    public List<CoreField.BarField> getQuarterlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        log.debug("历史行情15分钟数据：{}，{} -> {}", contract.getUnifiedSymbol(), startDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER), endDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER));
        return null;
    }

    @Override
    public List<CoreField.BarField> getHourlyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        log.debug("历史行情1小时数据：{}，{} -> {}", contract.getUnifiedSymbol(), startDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER), endDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER));
        return null;
    }

    @Override
    public List<CoreField.BarField> getDailyData(CoreField.ContractField contract, LocalDate startDate, LocalDate endDate) {
        log.debug("历史行情1天数据：{}，{} -> {}", contract.getUnifiedSymbol(), startDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER), endDate.format(DateTimeConstant.D_FORMAT_INT_FORMATTER));
        return null;
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
            for(int i=0; i<symbols.size(); i++) {
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
}
