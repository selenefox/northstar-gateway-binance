package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson.JSON;
import com.binance.connector.client.enums.DefaultUrls;
import com.binance.connector.client.impl.UMFuturesClientImpl;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.common.constant.TickType;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.common.model.core.Contract;
import org.dromara.northstar.common.model.core.Tick;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.MarketGateway;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreField;

@Slf4j
public class BinanceMarketGatewayAdapter extends GatewayAbstract implements MarketGateway {

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
        this.client = new UMWebsocketClientImpl(settings.isAccountType() ? DefaultUrls.USDM_WS_URL : DefaultUrls.USDM_UAT_WSS_URL);
        this.futuresClient = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey(), settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);
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
    public boolean subscribe(Contract contract) {
        log.debug("[{}] 币安网关订阅", gd.getGatewayId());

        String symbol = contract.symbol();
        //成交量精度
        double quantityPrecision = 1 / Math.pow(10, contract.quantityPrecision());

        int tickerStreamId = getSymbolStreams(symbol, contract, quantityPrecision);
        streamIdList.add(tickerStreamId);

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
                        .setUnifiedSymbol(contract.unifiedSymbol())
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

    /**
     * <br>Description:使用Streams订阅websocket，获取完整Ticker信息每2000毫秒推送、获取k线组装BAR数据、获取5档买卖单信息
     * <br>Author: 李嘉豪（lijiahao-zhixin@boss.com.cn）
     * <br>Date:2023年12月05日
     */

    private int getSymbolStreams(String symbol, Contract contract, double quantityPrecision) {
        ArrayList<String> streams = new ArrayList<>();
        String ticker = String.format("%s@ticker", symbol.toLowerCase());
        String kline = String.format("%s@kline_1m", symbol.toLowerCase());
        String depth = String.format("%s@depth5@500ms", symbol.toLowerCase());
        streams.add(ticker);
        streams.add(kline);
        streams.add(depth);

        List<Double> bidPriceList = new ArrayList<>();
        List<Double> askPriceList = new ArrayList<>();
        List<Integer> bidVolumeList = new ArrayList<>();
        List<Integer> askVolumeList = new ArrayList<>();
        return client.combineStreams(streams, ((event) -> {
            Map map = JSON.parseObject(event, Map.class);
            try {
                Map data = JSON.parseObject(JSON.toJSONString(map.get("data")), Map.class);
                if (depth.equals(map.get("stream"))) {
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
                    Long actionTimestamp = (Long) data.get("E");
                    Instant e = Instant.ofEpochMilli(actionTimestamp);
                    LocalTime actionTime = e.atZone(ZoneId.systemDefault()).toLocalTime();
                    LocalDate tradingDay = e.atZone(ZoneId.systemDefault()).toLocalDate();
                    LocalDate actionDay = LocalDate.now();

                    double volume = Double.parseDouble((String) data.get("v"));
                    double Q = Double.parseDouble((String) data.get("Q"));
                    double turnover = Double.parseDouble((String) data.get("q"));

                    Tick tick = Tick.builder()
                            .contract(contract)
                            .tradingDay(tradingDay)
                            .actionDay(actionDay)
                            .actionTime(actionTime)
                            .actionTimestamp(actionTimestamp)
                            .avgPrice(Double.valueOf(String.valueOf(data.get("w"))))
                            .highPrice(Double.valueOf(String.valueOf(data.get("h"))))
                            .lowPrice(Double.valueOf(String.valueOf(data.get("l"))))
                            .openPrice(Double.valueOf(String.valueOf(data.get("o"))))
                            .lastPrice(Double.valueOf(String.valueOf(data.get("c"))))
                            //.settlePrice(isReasonable(upperLimit, lowerLimit, settlePrice) ? settlePrice : preSettlePrice)
                            //.openInterest(openInterest)
                            //.openInterestDelta(openInterestDelta)
                            .volume((long) Q)
                            .volumeDelta((long) (Q / quantityPrecision))
                            .turnover(turnover)
                            .turnoverDelta(turnover)
                            //.lowerLimit(lowerLimit)
                            //.upperLimit(upperLimit)
                            //.preClosePrice(preClosePrice)
                            //.preSettlePrice(preSettlePrice)
                            //.preOpenInterest(preOpenInterest)
                            .askPrice(askPriceList)
                            .askVolume(askVolumeList)
                            .bidPrice(bidPriceList)
                            .bidVolume(bidVolumeList)
                            .gatewayId(gatewayId)
                            .channelType(ChannelType.BIAN)
                            .type(TickType.MARKET_TICK)
                            .build();
                    feEngine.emitEvent(NorthstarEventType.TICK, tick);
                    lastUpdateTickTime = System.currentTimeMillis();
                } else if (kline.equals(map.get("stream"))) {
                    Map k = JSON.parseObject(JSON.toJSONString(data.get("k")), Map.class);
                    if (!(Boolean) k.get("x")) {
                        return;
                    }
                    LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) data.get("E")), ZoneId.systemDefault()).withSecond(0).withNano(0);
                    String actionTime = dateTime.format(DateTimeConstant.T_FORMAT_FORMATTER);
                    String tradingDay = dateTime.format(DateTimeConstant.D_FORMAT_INT_FORMATTER);
                    double volume = Double.parseDouble((String) k.get("v")) / quantityPrecision; //成交量精度
                    double turnover = Double.parseDouble((String) k.get("q"));
                    Long numTrades = Long.valueOf(String.valueOf(k.get("n")));
                    CoreField.BarField bar = CoreField.BarField.newBuilder()
                            .setUnifiedSymbol(contract.unifiedSymbol())
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
                }

            } catch (Throwable t) {
                log.error("{} OnRtnDepthMarketData Exception", logInfo, t);
                //断练重新连接
                int klineStreamId = getSymbolStreams(symbol, contract, quantityPrecision);
                streamIdList.add(klineStreamId);
            }
        }));
    }

    @Override
    public boolean unsubscribe(Contract contract) {
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
