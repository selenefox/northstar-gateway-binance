package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson.JSON;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.common.constant.TickType;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.gateway.Contract;
import org.dromara.northstar.gateway.GatewayAbstract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.MarketGateway;
import org.dromara.northstar.gateway.TradeGateway;
import org.dromara.northstar.gateway.contract.GatewayContract;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.ContractField;

@Slf4j
public class BinanceMarketGatewayAdapter extends GatewayAbstract implements MarketGateway, TradeGateway {

    private UMWebsocketClientImpl client;

    private FastEventEngine feEngine;

    private IMarketCenter mktCenter;

    private GatewayDescription gd;

    protected ConnectionState connState = ConnectionState.DISCONNECTED;

    private List<Integer> streamIdList;

    private volatile long lastUpdateTickTime = System.currentTimeMillis();

    public BinanceMarketGatewayAdapter(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        super(gd, mktCenter);
        this.client = new UMWebsocketClientImpl();
        this.streamIdList = new ArrayList<>();
        this.feEngine = feEngine;
        this.gd = gd;
        this.mktCenter = mktCenter;
    }

    @Override
    public void connect() {
        log.debug("[{}] 币安网关连接", gd.getGatewayId());
        if (connState == ConnectionState.CONNECTED) {
            return;
        }
        mktCenter.loadContractGroup(ChannelType.BIAN);
        feEngine.emitEvent(NorthstarEventType.LOGGED_IN, gd.getGatewayId());
        feEngine.emitEvent(NorthstarEventType.GATEWAY_READY, gatewayId);

        connState = ConnectionState.CONNECTED;
    }

    @Override
    public void disconnect() {
        Iterator<Integer> iterator = streamIdList.iterator();
        while (iterator.hasNext()) {
            client.closeConnection(iterator.next());
            iterator.remove();
        }
        feEngine.emitEvent(NorthstarEventType.LOGGING_OUT, gatewayId);
        feEngine.emitEvent(NorthstarEventType.LOGGED_OUT, gatewayId);
        connState = ConnectionState.DISCONNECTED;
    }

    @Override
    public boolean getAuthErrorFlag() {
        return true;
    }

    @Override
    public boolean subscribe(ContractField contractField) {
        log.debug("[{}] 币安网关订阅", gd.getGatewayId());
        String symbol = contractField.getSymbol();
        Contract contract = mktCenter.getContract(ChannelType.BIAN, symbol);
        //成交量精度
        double quantityPrecision = 1 / Math.pow(10, contract.contractField().getQuantityPrecision());

        int tickerStreamId = getSymbolTicker(symbol, contract, quantityPrecision);
        int klineStreamId = getKlineStream(symbol, contract, quantityPrecision);
        streamIdList.add(tickerStreamId);
        streamIdList.add(klineStreamId);

        return true;
    }

    //使用币安提供的k线，有问题会出现两条k线
    private int getKlineStream(String symbol, Contract contract, double quantityPrecision) {

        return client.klineStream(symbol, "1m", ((event) -> {
            Map map = JSON.parseObject(event, Map.class);
            Map k = JSON.parseObject(JSON.toJSONString(map.get("k")), Map.class);
            try {
                if (!(Boolean) k.get("x")) {
                    return;
                }
                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) map.get("E")), ZoneId.systemDefault()).withSecond(0).withNano(0);
                String actionTime = dateTime.format(DateTimeConstant.T_FORMAT_FORMATTER);
                String tradingDay = dateTime.format(DateTimeConstant.D_FORMAT_INT_FORMATTER);
                double volume = Double.parseDouble((String) k.get("v")) / quantityPrecision; //成交量精度
                double turnover = Double.parseDouble((String) k.get("q"));
                Long numTrades = Long.valueOf(String.valueOf(k.get("n")));
                CoreField.BarField bar = CoreField.BarField.newBuilder()
                        .setUnifiedSymbol(contract.contractField().getUnifiedSymbol())
                        .setGatewayId(gatewayId)
                        .setTradingDay(tradingDay)
                        .setActionDay(tradingDay)
                        .setActionTime(actionTime)
                        .setActionTimestamp(dateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli())
                        .setOpenPrice(Double.valueOf(String.valueOf(k.get("o"))))
                        .setHighPrice(Double.valueOf(String.valueOf(k.get("h"))))
                        .setLowPrice(Double.valueOf(String.valueOf(k.get("l"))))
                        .setClosePrice(Double.valueOf(String.valueOf(k.get("c"))))
                        .setVolume((long) volume)
                        .setVolumeDelta((long) volume)
                        .setTurnover(turnover)
                        .setTurnoverDelta(turnover)
                        .setNumTrades(numTrades)
                        .setNumTradesDelta(numTrades)
                        .setChannelType(ChannelType.BIAN.toString())
                        .build();
                feEngine.emitEvent(NorthstarEventType.BAR, bar);
            } catch (Throwable t) {
                log.error("{} OnRtnDepthMarketData Exception", logInfo, t);
                //断练重新连接
                int klineStreamId = getKlineStream(symbol, contract, quantityPrecision);
                streamIdList.add(klineStreamId);
            }
        }));
    }

    //精简信息 每500毫秒推送
    private int getMiniTickerStream(String symbol, GatewayContract contract, String format, String actionTime) {
        return client.miniTickerStream(symbol, ((event) -> {
            Map map = JSON.parseObject(event, Map.class);
            try {
                ContractField c = contract.contractField();
                CoreField.TickField.Builder tickBuilder = CoreField.TickField.newBuilder();
                tickBuilder.setUnifiedSymbol(c.getUnifiedSymbol());
                tickBuilder.setGatewayId(gatewayId);
                tickBuilder.setTradingDay(format);
                tickBuilder.setActionDay(format);
                tickBuilder.setActionTime(actionTime);
                tickBuilder.setActionTimestamp((Long) map.get("E"));
                tickBuilder.setStatus(TickType.NORMAL_TICK.getCode());
                tickBuilder.setLastPrice(Double.valueOf(String.valueOf(map.get("c"))));
                //tickBuilder.setAvgPrice(Double.valueOf(String.valueOf(map.get("w"))));
                tickBuilder.setHighPrice(Double.valueOf(String.valueOf(map.get("h"))));
                tickBuilder.setLowPrice(Double.valueOf(String.valueOf(map.get("l"))));
                tickBuilder.setOpenPrice(Double.valueOf(String.valueOf(map.get("o"))));
                double volume = Double.parseDouble((String) map.get("v"));
                double turnover = Double.parseDouble((String) map.get("q"));

                tickBuilder.setVolume((long) volume);
                tickBuilder.setTurnover((long) turnover);

//                tickBuilder.setBidPrice(Double.valueOf(String.valueOf(map.get("c"))));
//                tickBuilder.setAskPrice(Double.valueOf(String.valueOf(map.get("c"))));

                tickBuilder.setChannelType(ChannelType.BIAN.toString());
                CoreField.TickField tick = tickBuilder.build();
                feEngine.emitEvent(NorthstarEventType.TICK, tick);
                contract.onTick(tick);
            } catch (Throwable t) {
                log.error("{} OnRtnDepthMarketData Exception", logInfo, t);
            }
        }));
    }

    //完整信息 每2000毫秒推送
    private int getSymbolTicker(String symbol, Contract contract, double quantityPrecision) {
        return client.symbolTicker(symbol, ((event) -> {
            ContractField c = contract.contractField();
            Map map = JSON.parseObject(event, Map.class);
            try {
                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) map.get("E")), ZoneId.systemDefault());
                String actionTime = dateTime.format(DateTimeConstant.T_FORMAT_WITH_MS_INT_FORMATTER);
                String tradingDay = dateTime.format(DateTimeConstant.D_FORMAT_INT_FORMATTER);
                CoreField.TickField.Builder tickBuilder = CoreField.TickField.newBuilder();
                tickBuilder.setUnifiedSymbol(c.getUnifiedSymbol());
                tickBuilder.setGatewayId(gatewayId);
                tickBuilder.setTradingDay(tradingDay);
                tickBuilder.setActionDay(tradingDay);
                tickBuilder.setActionTime(actionTime);
                tickBuilder.setActionTimestamp((Long) map.get("E"));
                tickBuilder.setStatus(TickType.NORMAL_TICK.getCode());
                tickBuilder.setLastPrice(Double.valueOf(String.valueOf(map.get("c"))));
                tickBuilder.setAvgPrice(Double.valueOf(String.valueOf(map.get("w"))));
                tickBuilder.setHighPrice(Double.valueOf(String.valueOf(map.get("h"))));
                tickBuilder.setLowPrice(Double.valueOf(String.valueOf(map.get("l"))));
                tickBuilder.setOpenPrice(Double.valueOf(String.valueOf(map.get("o"))));
                double volume = Double.parseDouble((String) map.get("v"));
                double Q = Double.parseDouble((String) map.get("Q"));
                double turnover = Double.parseDouble((String) map.get("q"));

                tickBuilder.setVolume((long) Q);
                //成交量按照最小交易单位数量实现
                tickBuilder.setVolumeDelta((long) (Q / quantityPrecision));
                tickBuilder.setTurnover(turnover);
                tickBuilder.setTurnoverDelta(turnover);

//                tickBuilder.setBidPrice(Double.valueOf(String.valueOf(map.get("c"))));
//                tickBuilder.setAskPrice(Double.valueOf(String.valueOf(map.get("c"))));

                tickBuilder.setChannelType(ChannelType.BIAN.toString());
                CoreField.TickField tick = tickBuilder.build();
                feEngine.emitEvent(NorthstarEventType.TICK, tick);
                lastUpdateTickTime = System.currentTimeMillis();
            } catch (Throwable t) {
                //断练重新连接
                int klineStreamId = getSymbolTicker(symbol, contract, quantityPrecision);
                streamIdList.add(klineStreamId);
            }
        }));
    }

    @Override
    public boolean unsubscribe(ContractField contract) {
        Iterator<Integer> iterator = streamIdList.iterator();
        while (iterator.hasNext()) {
            client.closeConnection(iterator.next());
            iterator.remove();
        }
        return true;
    }

    @Override
    public boolean isActive() {
        return System.currentTimeMillis() - lastUpdateTickTime < 1000;
    }

    @Override
    public ChannelType channelType() {
        return ChannelType.BIAN;
    }

    @Override
    public GatewayDescription gatewayDescription() {
        gd.setConnectionState(getConnectionState());
        return gd;
    }

    @Override
    public String gatewayId() {
        return gd.getGatewayId();
    }

    @Override
    public ConnectionState getConnectionState() {
        return connState;
    }

    @Override
    public String submitOrder(CoreField.SubmitOrderReqField submitOrderReq) {
        return null;
    }

    @Override
    public boolean cancelOrder(CoreField.CancelOrderReqField cancelOrderReq) {
        return false;
    }
}
