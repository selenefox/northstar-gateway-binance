package org.dromara.northstar.gateway.binance;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.constant.TickType;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.common.model.core.Bar;
import org.dromara.northstar.common.model.core.Contract;
import org.dromara.northstar.common.model.core.Tick;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.MarketGateway;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.enums.DefaultUrls;
import com.binance.connector.client.impl.UMFuturesClientImpl;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import lombok.extern.slf4j.Slf4j;

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

        int tickerStreamId = getSymbolStreams(symbol, contract);
        streamIdList.add(tickerStreamId);

        return true;
    }


    /**
     * <br>Description:使用Streams订阅websocket，获取完整Ticker信息每2000毫秒推送、获取k线组装BAR数据、获取5档买卖单信息
     * <br>Author: 李嘉豪（lijiahao-zhixin@boss.com.cn）
     * <br>Date:2023年12月05日
     */

    private int getSymbolStreams(String symbol, Contract contract) {
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
        //成交量精度
        double quantityPrecision = 1 / Math.pow(10, contract.quantityPrecision());
        return client.combineStreams(streams, ((event) -> {
            JSONObject jsonObject = JSON.parseObject(event);
            try {
                String stream = jsonObject.getString("stream");
                JSONObject data = jsonObject.getJSONObject("data");
                if (depth.equals(stream)) {
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
                } else if (ticker.equals(stream)) {
                    Long actionTimestamp = data.getLong("E");
                    Instant e = Instant.ofEpochMilli(actionTimestamp);
                    LocalTime actionTime = e.atZone(ZoneId.systemDefault()).toLocalTime();
                    LocalDate tradingDay = e.atZone(ZoneId.systemDefault()).toLocalDate();

                    double volume = data.getDoubleValue("v");
                    double Q = data.getDoubleValue("Q");
                    double turnover = data.getDoubleValue("q");

                    Tick tick = Tick.builder()
                            .contract(contract)
                            .tradingDay(tradingDay)
                            .actionDay(tradingDay)
                            .actionTime(actionTime)
                            .actionTimestamp(actionTimestamp)
                            .avgPrice(data.getDouble("w"))
                            .highPrice(data.getDouble("h"))
                            .lowPrice(data.getDouble("l"))
                            .openPrice(data.getDouble("o"))
                            .lastPrice(data.getDouble("c"))
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
                    mktCenter.onTick(tick);
                    feEngine.emitEvent(NorthstarEventType.TICK, tick);
                    lastUpdateTickTime = System.currentTimeMillis();
                } else if (kline.equals(stream)) {
                    JSONObject k = data.getJSONObject("k");
                    if (!k.getBoolean("x")) {
                        return;
                    }
                    Long actionTimestamp = k.getLong("T") + 1;
                    Instant e = Instant.ofEpochMilli(actionTimestamp);
                    LocalTime actionTime = e.atZone(ZoneId.systemDefault()).toLocalTime();
                    LocalDate tradingDay = e.atZone(ZoneId.systemDefault()).toLocalDate();
                    double volume = k.getDoubleValue("v") / quantityPrecision; //成交量精度
                    double turnover = k.getDoubleValue("q");
                    Long numTrades = k.getLong("n");
                    Bar bar = Bar.builder()
                            .contract(contract)
                            .gatewayId(gatewayId)
                            .tradingDay(tradingDay)
                            .actionDay(tradingDay)
                            .actionTime(actionTime)
                            .actionTimestamp(actionTimestamp)
                            .openPrice(k.getDoubleValue("o"))
                            .highPrice(k.getDoubleValue("h"))
                            .lowPrice(k.getDoubleValue("l"))
                            .closePrice(k.getDoubleValue("c"))
                            .volume((long) volume)
                            .volumeDelta((long) volume)
                            .turnover(turnover)
                            .turnoverDelta(turnover)
                            .channelType(ChannelType.BIAN)
                            .build();
                    feEngine.emitEvent(NorthstarEventType.BAR, bar);
                }

            } catch (Throwable t) {
                log.error("{} OnRtnDepthMarketData Exception", logInfo, t);
                //断练重新连接
                int klineStreamId = getSymbolStreams(symbol, contract);
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
