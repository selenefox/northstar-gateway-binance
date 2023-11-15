package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson.JSON;
import com.binance.connector.client.impl.UMFuturesClientImpl;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.ContractField;

@Slf4j
public class BinanceMarketGatewayAdapter extends GatewayAbstract implements MarketGateway{

    private UMWebsocketClientImpl client;

    private UMFuturesClientImpl futuresClient;

    private FastEventEngine feEngine;

    private IMarketCenter mktCenter;

    private GatewayDescription gd;

    protected ConnectionState connState = ConnectionState.DISCONNECTED;

    private List<Integer> streamIdList;

    private volatile long lastUpdateTickTime = System.currentTimeMillis();

    private Timer statusReportTimer;

    public BinanceMarketGatewayAdapter(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        super(gd, mktCenter);
        BinanceGatewaySettings settings = (BinanceGatewaySettings) gd.getSettings();
        this.client = new UMWebsocketClientImpl();
        this.futuresClient = new UMFuturesClientImpl(settings.getApiKey(),settings.getSecretKey());
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
    /**
     * <br>Description:获取k线组装BAR数据
     * <br>Author: 李嘉豪
     * <br>Date:2023年10月30日
     */
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

    //获取完整Ticker信息 每2000毫秒推送
    private int getSymbolTicker(String symbol, Contract contract, double quantityPrecision) {
        ArrayList<String> streams = new ArrayList<>();
        String ticker = String.format("%s@ticker", symbol.toLowerCase());
        String depth = String.format("%s@depth5@500ms", symbol.toLowerCase());
        streams.add(ticker);
        streams.add(depth);

        List<Double> bidPriceList = new ArrayList<>();
        List<Double> askPriceList = new ArrayList<>();
        List<Integer> bidVolumeList = new ArrayList<>();
        List<Integer> askVolumeList = new ArrayList<>();
        return client.combineStreams(streams, ((event) -> {
            ContractField c = contract.contractField();
            Map map = JSON.parseObject(event, Map.class);
            try {
                if (depth.equals(map.get("stream"))) {
                    Map data = JSON.parseObject(JSON.toJSONString(map.get("data")), Map.class);
                    // 从Map中获取买方和卖方的数据
                    List<List<String>> bids = (List<List<String>>) data.get("b");
                    List<List<String>> asks = (List<List<String>>) data.get("a");
                    bidPriceList.clear();
                    askPriceList.clear();
                    bidVolumeList.clear();
                    askVolumeList.clear();

                    // 分别提取买方和卖方的价格和数量
                    for (List<String> bid : bids) {
                        bidPriceList.add(Double.parseDouble(bid.get(0)));
                        bidVolumeList.add((int) (Double.parseDouble(bid.get(1)) / quantityPrecision));
                    }

                    for (List<String> ask : asks) {
                        askPriceList.add(Double.parseDouble(ask.get(0)));
                        askVolumeList.add((int) (Double.parseDouble(ask.get(1)) / quantityPrecision));
                    }
                } else if (ticker.equals(map.get("stream"))) {
                    Map data = JSON.parseObject(JSON.toJSONString(map.get("data")), Map.class);
                    LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) data.get("E")), ZoneId.systemDefault());
                    String actionTime = dateTime.format(DateTimeConstant.T_FORMAT_WITH_MS_INT_FORMATTER);
                    String tradingDay = dateTime.format(DateTimeConstant.D_FORMAT_INT_FORMATTER);
                    CoreField.TickField.Builder tickBuilder = CoreField.TickField.newBuilder();
                    tickBuilder.setUnifiedSymbol(c.getUnifiedSymbol());
                    tickBuilder.setGatewayId(gatewayId);
                    tickBuilder.setTradingDay(tradingDay);
                    tickBuilder.setActionDay(tradingDay);
                    tickBuilder.setActionTime(actionTime);
                    tickBuilder.setActionTimestamp((Long) data.get("E"));
                    tickBuilder.setStatus(TickType.NORMAL_TICK.getCode());
                    tickBuilder.setLastPrice(Double.valueOf(String.valueOf(data.get("c"))));
                    tickBuilder.setAvgPrice(Double.valueOf(String.valueOf(data.get("w"))));
                    tickBuilder.setHighPrice(Double.valueOf(String.valueOf(data.get("h"))));
                    tickBuilder.setLowPrice(Double.valueOf(String.valueOf(data.get("l"))));
                    tickBuilder.setOpenPrice(Double.valueOf(String.valueOf(data.get("o"))));
                    double volume = Double.parseDouble((String) data.get("v"));
                    double Q = Double.parseDouble((String) data.get("Q"));
                    double turnover = Double.parseDouble((String) data.get("q"));

                    tickBuilder.setVolume((long) Q);
                    //成交量按照最小交易单位数量实现
                    tickBuilder.setVolumeDelta((long) (Q / quantityPrecision));
                    tickBuilder.setTurnover(turnover);
                    tickBuilder.setTurnoverDelta(turnover);

                    tickBuilder.addAllAskPrice(askPriceList);
                    tickBuilder.addAllBidPrice(bidPriceList);
                    tickBuilder.addAllAskVolume(askVolumeList);
                    tickBuilder.addAllBidVolume(bidVolumeList);

                    tickBuilder.setChannelType(ChannelType.BIAN.toString());
                    CoreField.TickField tick = tickBuilder.build();
                    feEngine.emitEvent(NorthstarEventType.TICK, tick);
                    lastUpdateTickTime = System.currentTimeMillis();
                }

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

}
