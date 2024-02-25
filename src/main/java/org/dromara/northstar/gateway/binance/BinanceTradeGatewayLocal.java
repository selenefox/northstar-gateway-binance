package org.dromara.northstar.gateway.binance;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.enums.DefaultUrls;
import com.binance.connector.client.impl.UMFuturesClientImpl;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.exception.TradeException;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.common.model.core.Account;
import org.dromara.northstar.common.model.core.Contract;
import org.dromara.northstar.common.model.core.Order;
import org.dromara.northstar.common.model.core.Position;
import org.dromara.northstar.common.model.core.SubmitOrderReq;
import org.dromara.northstar.common.model.core.Trade;
import org.dromara.northstar.gateway.IContract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.TradeGateway;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import cn.hutool.core.util.ObjectUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreEnum;


@Slf4j
public class BinanceTradeGatewayLocal implements TradeGateway {

    protected FastEventEngine feEngine;

    private static final Logger logger = LoggerFactory.getLogger(BinanceTradeGatewayLocal.class);

    @Getter
    private boolean connected;

    private GatewayDescription gd;

    private ConnectionState connState = ConnectionState.DISCONNECTED;

    private Timer statusReportTimer;

    private IMarketCenter mktCenter;

    private BinanceGatewaySettings settings;

    private UMFuturesClientImpl futuresClient;

    private UMWebsocketClientImpl websocketClient;

    protected Map<String, Order> orderMap = new HashMap<>();

    protected Map<String, SubmitOrderReq> submitOrderReqFieldMap = new HashMap<>();

    private List<Integer> streamIdList = new ArrayList<>();

    private Map<String, JSONObject> positionMap = new HashMap<>();

    public BinanceTradeGatewayLocal(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        this.settings = (BinanceGatewaySettings) gd.getSettings();
        this.futuresClient = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey(), settings.isAccountType() ? DefaultUrls.USDM_PROD_URL : DefaultUrls.USDM_UAT_URL);
        this.websocketClient = new UMWebsocketClientImpl(settings.isAccountType() ? DefaultUrls.USDM_WS_URL : DefaultUrls.USDM_UAT_WSS_URL);
        this.feEngine = feEngine;
        this.gd = gd;
        this.mktCenter = mktCenter;
    }

    @Override
    public void connect() {
        log.debug("[{}] 账户网关连线", gd.getGatewayId());
        connected = true;
        connState = ConnectionState.CONNECTED;
        feEngine.emitEvent(NorthstarEventType.LOGGED_IN, gd.getGatewayId());
        CompletableFuture.runAsync(() -> feEngine.emitEvent(NorthstarEventType.GATEWAY_READY, gd.getGatewayId()),
                CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS));

        //获取账户信息
        AtomicReference<JSONObject> jsonObject = new AtomicReference<>(getAccountInformation());

        //查询全部挂单
        currentAllOpenOrders();

        //生成listenKey
        String listen = futuresClient.userData().createListenKey();
        JSONObject jsonListenKey = JSON.parseObject(listen);

        try {
            //Websocket 账户信息推送
            streamIdList.add(listenUserStream(jsonListenKey, jsonObject));
        } catch (Exception t) {
            //断练重新连接
            t.getStackTrace();
            log.error("账户信息推送断练重新连接");
            streamIdList.add(listenUserStream(jsonListenKey, jsonObject));
        }

        statusReportTimer = new Timer("BinanceGatewayTimelyReport", true);
        statusReportTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                //延长listenKey有效期
                futuresClient.userData().extendListenKey();
            }
        }, 5000, 1800000);
        statusReportTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                String result = getAccountInformation();
                JSONObject accountInformation = JSON.parseObject(result);
                //账户事件
                getAccountField(accountInformation);
                JSONArray positions = accountInformation.getJSONArray("positions");
                List<JSONObject> positionList = positions.stream().map(item -> (JSONObject) item).filter(item -> item.getDouble("positionAmt") != 0).collect(Collectors.toList());
                //当前持仓map
                Map<String, JSONObject> newPositionMap = positionList.stream().collect(Collectors.toMap(dto -> dto.getString("symbol") + "@" + dto.getString("positionSide"), Function.identity(), (a, b) -> a, HashMap::new));
                //新持仓加入到positionMap
                for (JSONObject position : positionList) {
                    String symbol = position.getString("symbol");
                    String positionSide = position.getString("positionSide");
                    String key = symbol + "@" + positionSide;
                    if (!positionMap.containsKey(key)) {
                        positionMap.put(key, position);
                    }
                }
                //查出平仓的合约发送持仓事件-持仓数量为0
                Iterator<String> iterator = positionMap.keySet().iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    // positionMap中存在但是newPositionMap没有的position
                    if (!newPositionMap.containsKey(key)) {
                        JSONObject closeAPosition = positionMap.get(key);
                        closeAPosition.put("positionAmt", 0);
                        positionList.add(closeAPosition);
                    }
                }
                for (JSONObject position : positionList) {
                    String symbol = position.getString("symbol");
                    String positionSide = position.getString("positionSide");
                    CoreEnum.PositionDirectionEnum posDir = "LONG".equals(positionSide) ? CoreEnum.PositionDirectionEnum.PD_Long : CoreEnum.PositionDirectionEnum.PD_Short;
                    IContract contract = mktCenter.getContract(ChannelType.BIAN, symbol);
                    Contract contracted = contract.contract();
                    //持仓数量按照最小交易精度转换
                    int positionAmt = Math.abs(Double.valueOf(position.getDouble("positionAmt") / contracted.multiplier()).intValue());
                    Double useMargin = position.getDouble("positionInitialMargin");
                    Double unrealizedProfit = position.getDouble("unrealizedProfit");
                    Double positionInitialMargin = position.getDouble("positionInitialMargin");
                    Position pos = Position.builder()
                            .positionId(contracted.unifiedSymbol() + "@" + posDir)
                            .gatewayId(gd.getGatewayId())
                            .positionDirection(posDir)
                            .position(positionAmt)
                            .tdPosition(positionAmt)
                            .ydPosition(positionAmt)
                            .contract(contracted)
                            //.frozen(frozen)
                            //.tdFrozen(tdFrozen)
                            //.ydFrozen(ydFrozen)
                            .openPrice(position.getDouble("entryPrice"))
                            .openPriceDiff(unrealizedProfit)
                            .positionProfit(unrealizedProfit)
                            .positionProfitRatio(useMargin == 0 ? 0 : unrealizedProfit / useMargin)
                            .contractValue(unrealizedProfit)
                            .useMargin(positionInitialMargin)
                            .exchangeMargin(positionInitialMargin)
                            .updateTimestamp(position.getLong("updateTime"))
                            .build();
                    logger.trace("合成持仓对象：{}", JSON.toJSONString(pos));
                    feEngine.emitEvent(NorthstarEventType.POSITION, pos);
                }
            }

            private String getAccountInformation() {
                try {
                    return futuresClient.account().accountInformation(new LinkedHashMap<>());
                } catch (Exception e) {
                    log.error("{} getAccountInformation Exception", e);
                    try {
                        Thread.sleep(3000);
                    } catch (Exception exception) {
                        log.error("{} getAccountInformation Exception", exception);
                    }
                    return futuresClient.account().accountInformation(new LinkedHashMap<>());
                }
            }
        }, 5000, 3000);
    }

    private void currentAllOpenOrders() {
        String result = futuresClient.account().currentAllOpenOrders(new LinkedHashMap<>());
        List<JSONObject> openOrderList = JSON.parseArray(result).stream().map(item -> (JSONObject) item).collect(Collectors.toList());
        //维护订单ID和symbol的map
        for (JSONObject order : openOrderList) {
            IContract contract = mktCenter.getContract(ChannelType.BIAN, order.getString("symbol"));
            Contract contracted = contract.contract();
            String side = order.getString("side");
            String positionSide = order.getString("positionSide");
            CoreEnum.DirectionEnum dBuy = null;
            CoreEnum.OffsetFlagEnum offsetFlag = null;
            if ("BUY".equals(side)) {
                dBuy = CoreEnum.DirectionEnum.D_Buy;
                offsetFlag = CoreEnum.OffsetFlagEnum.OF_Open;
            } else if ("SELL".equals(side)) {
                dBuy = CoreEnum.DirectionEnum.D_Sell;
                offsetFlag = CoreEnum.OffsetFlagEnum.OF_Close;
            }
            Order orderBuilder = getOrderBuilder(dBuy, order, contracted, offsetFlag);
            orderMap.put(order.getString("clientOrderId"), orderBuilder);
        }
    }

    private int listenUserStream(JSONObject jsonListenKey, AtomicReference<JSONObject> jsonObject) {
        //账户信息推送
        return websocketClient.listenUserStream(jsonListenKey.getString("listenKey"), ((event) -> {
            log.info("账户信息推送:[{}]", event);
            JSONObject eventJson = JSON.parseObject(event);
            switch (eventJson.getString("e")) {
                case "ORDER_TRADE_UPDATE" -> jsonObject.set(orderTradeUpdate(eventJson));
                case "ACCOUNT_UPDATE" -> jsonObject.set(accountUpdate(eventJson));
            }

        }));
    }

    @Nullable
    private JSONObject getAccountInformation() {
        String result = futuresClient.account().accountInformation(new LinkedHashMap<>());
        //更新合约多头空头保证金率，添加持仓回报事件
        JSONObject jsonObject = JSON.parseObject(result);
        return jsonObject;
    }

    @NotNull
    private void getAccountField(JSONObject jsonObject) {
        Account accountBuilder = Account.builder()
                .accountId(gd.getGatewayId())
                .available(jsonObject.getDouble("availableBalance"))
                .balance(jsonObject.getDouble("totalCrossWalletBalance"))
                .closeProfit(jsonObject.getDouble("totalUnrealizedProfit"))
                //TODO，ETH/BTC期货合约将按照BUSD手续费表计。这里币安返回的feeTier是手续费等级,0=0.0200%/0.0500%(USDT-Maker / Taker),暂时写死后续处理
                .commission(Double.valueOf(0.0002))
                .gatewayId(gd.getGatewayId())
                .currency(CoreEnum.CurrencyEnum.USD)
                .margin(jsonObject.getDouble("totalInitialMargin"))
                .positionProfit(jsonObject.getDouble("totalCrossUnPnl"))
                .build();
        feEngine.emitEvent(NorthstarEventType.ACCOUNT, accountBuilder);
    }

    //Balance和Position更新推送
    private JSONObject accountUpdate(JSONObject eventJson) {
        return getAccountInformation();
    }

    //订单/交易 更新推送
    private JSONObject orderTradeUpdate(JSONObject json) {
        JSONObject o = json.getJSONObject("o");
        //订单当前状态
        String X = o.getString("X");
        //订单id
        String c = o.getString("c");
        String s = o.getString("s");
        //方向
        String S = o.getString("S");
        //成交时间
        Long T = o.getLong("T");
        //订单原始数量
        Double q = o.getDouble("q");
        //订单末次成交量
        Double l = o.getDouble("l");
        //订单末次成交价格
        Double L = o.getDouble("L");
        SubmitOrderReq orderReq = submitOrderReqFieldMap.get(c);

        Contract contract = orderReq.contract();
        Instant e = Instant.ofEpochMilli(T);

        LocalTime tradeTime = e.atZone(ZoneId.systemDefault()).toLocalTime();
        LocalDate tradeDate = e.atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate tradingDay = LocalDate.now();
        //订单末次成交量按照最小交易精度转换
        int executedQty = Math.abs(Double.valueOf(l / contract.multiplier()).intValue());
        //订单原始数量
        int origQty = Math.abs(Double.valueOf(q / contract.multiplier()).intValue());

        if (ObjectUtil.isNotEmpty(orderReq)) {

            Trade trade = Trade.builder()
                    .tradeDate(tradeDate)
                    .tradeTime(tradeTime)
                    .tradingDay(tradingDay)
                    .tradeTimestamp(T)
                    .direction(orderReq.direction())
                    .offsetFlag(orderReq.offsetFlag())
                    .contract(orderReq.contract())
                    .orderId(orderReq.originOrderId())
                    .originOrderId(orderReq.originOrderId())
                    .price(L)
                    .volume(orderReq.volume())
                    .gatewayId(orderReq.gatewayId())
                    .tradeType(CoreEnum.TradeTypeEnum.TT_Common)
                    .priceSource(CoreEnum.PriceSourceEnum.PSRC_LastPrice)
                    .build();

            CoreEnum.DirectionEnum dBuy = null;
            CoreEnum.OffsetFlagEnum offsetFlag = null;
            if ("BUY".equals(S)) {
                dBuy = CoreEnum.DirectionEnum.D_Buy;
                offsetFlag = CoreEnum.OffsetFlagEnum.OF_Open;
            } else if ("SELL".equals(S)) {
                dBuy = CoreEnum.DirectionEnum.D_Sell;
                offsetFlag = CoreEnum.OffsetFlagEnum.OF_Close;
            }
            Order.OrderBuilder buildered = Order.builder();

            buildered.orderId(c);
            buildered.originOrderId(c);
            buildered.gatewayId(gd.getGatewayId());
            buildered.contract(contract);
            buildered.direction(dBuy);
            buildered.offsetFlag(offsetFlag);
            buildered.price(L);
            buildered.totalVolume(origQty);
            buildered.tradedVolume(executedQty);
            buildered.tradingDay(tradingDay);
            buildered.updateTime(LocalTime.now());
            buildered.orderDate(LocalDate.now());
            //FILLED 全部成交
            if (X.equals("FILLED")) {
                buildered.statusMsg("全部成交");
                buildered.orderStatus(CoreEnum.OrderStatusEnum.OS_AllTraded);

            } else if (X.equals("CANCELED")) {
                buildered.statusMsg("已撤单");
                buildered.orderStatus(CoreEnum.OrderStatusEnum.OS_Canceled);
            } else if (X.equals("NEW")) {
                //储存挂单信息
                orderMap.put(c, buildered.build());
                buildered.statusMsg("已报单").orderStatus(CoreEnum.OrderStatusEnum.OS_Unknown);
            } else if (X.equals("PARTIALLY_FILLED")) {

            }
            Order build = buildered.build();
            feEngine.emitEvent(NorthstarEventType.ORDER, build);
            if (X.equals("FILLED")) {
                feEngine.emitEvent(NorthstarEventType.TRADE, trade);
            }
            log.info("[{}] 订单反馈：{} {} {} {} {}", build.gatewayId(), build.orderDate(), build.updateTime(), build.originOrderId(), build.orderStatus(), build.statusMsg());
        }
        return getAccountInformation();
    }

    @Override
    public void disconnect() {
        log.debug("[{}] 模拟网关断开", gd.getGatewayId());
        Iterator<Integer> iterator = streamIdList.iterator();
        connected = false;
        connState = ConnectionState.DISCONNECTED;
        while (iterator.hasNext()) {
            websocketClient.closeConnection(iterator.next());
            iterator.remove();
        }
        statusReportTimer.cancel();
    }

    @Override
    public ConnectionState getConnectionState() {
        return connState;
    }

    @Override
    public String submitOrder(SubmitOrderReq submitOrderReq) throws TradeException {
        if (!isConnected()) {
            throw new IllegalStateException("网关未连线");
        }
        log.info("[{}] 网关收到下单请求,参数:[{}]", gd.getGatewayId(), submitOrderReq);
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();

        Contract contract = submitOrderReq.contract();
        CoreEnum.DirectionEnum direction = submitOrderReq.direction();
        CoreEnum.OffsetFlagEnum offsetFlag = submitOrderReq.offsetFlag();
        CoreEnum.TimeConditionEnum timeCondition = submitOrderReq.timeCondition();
        //数量 * 最小交易数量
        double quantity = submitOrderReq.volume() * contract.multiplier();
        String side;
        String positionSide;
        String timeInForce;
        String type;
        //有效方式
        switch (timeCondition) {
            case TC_IOC -> timeInForce = "IOC";
            case TC_GTD -> timeInForce = "GTD";
            case TC_GTC -> timeInForce = "GTC";
            default -> timeInForce = "GTC";
        }
        // 开仓
        //开多：买多BUY、LONG
        //开空：卖空SELL、SHORT
        if (CoreEnum.OffsetFlagEnum.OF_Open.getNumber() == offsetFlag.getNumber()) {
            side = (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) ? "BUY" : "SELL";
            type = (submitOrderReq.price() == 0) ? "MARKET" : "LIMIT";

            if ("LIMIT".equals(type)) {
                parameters.put("timeInForce", timeInForce);
                parameters.put("price", submitOrderReq.price());
            }

            // 持仓方向
            positionSide = (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) ? "LONG" : "SHORT";
            parameters.put("positionSide", positionSide);
        } else {
            // 平仓
            //平空：卖空BUY、SHORT
            //平多：卖多SELL、LONG
            side = (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) ? "BUY" : "SELL";
            type = (submitOrderReq.price() == 0) ? "MARKET" : "LIMIT";

            // 平仓方向
            positionSide = (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) ? "SHORT" : "LONG";
            if ("LIMIT".equals(type)) {
                parameters.put("price", submitOrderReq.price());
                parameters.put("timeInForce", timeInForce);
            }
            parameters.put("positionSide", positionSide);
        }
        //订单种类,市价单不传价格

        parameters.put("symbol", contract.symbol());
        parameters.put("side", side);
        parameters.put("type", type);
        parameters.put("quantity", quantity);
        parameters.put("newClientOrderId", submitOrderReq.originOrderId());

        submitOrderReqFieldMap.put(submitOrderReq.originOrderId(), submitOrderReq);
        //向币安提交订单
        String s = futuresClient.account().newOrder(parameters);
        log.info("[{}] 网关收到下单返回,响应:[{}]", gd.getGatewayId(), s);

        JSONObject orderJson = JSON.parseObject(s);
        //查询全部挂单
        //currentAllOpenOrders();
        return submitOrderReq.originOrderId();
    }

    @NotNull
    private Order getOrderBuilder(CoreEnum.DirectionEnum directionEnum, JSONObject orderJson, Contract contract, CoreEnum.OffsetFlagEnum offsetFlag) {
        Order.OrderBuilder orderBuilder = Order.builder();

        //数量 * 最小交易数量
        int origQty = (int) Math.round(orderJson.getDouble("origQty") * Math.pow(10, contract.quantityPrecision()));
        int executedQty = (int) Math.round(orderJson.getDouble("executedQty") * Math.pow(10, contract.quantityPrecision()));
        orderBuilder.orderId(orderJson.getString("clientOrderId"));
        orderBuilder.originOrderId(orderJson.getString("clientOrderId"));
        orderBuilder.direction(directionEnum);
        orderBuilder.offsetFlag(offsetFlag);
        //orderBuilder.setOrderStatus(CoreEnum.OrderStatusEnum.OS_AllTraded);
        orderBuilder.price(orderJson.getDouble("price"));
        orderBuilder.totalVolume(origQty);
        orderBuilder.tradedVolume(executedQty);
        orderBuilder.contract(contract);
        orderBuilder.gatewayId(gd.getGatewayId());
        orderBuilder.statusMsg("已报单").orderStatus(CoreEnum.OrderStatusEnum.OS_Unknown);
        orderBuilder.updateTime(LocalTime.now());
        Order order = orderBuilder.build();
        feEngine.emitEvent(NorthstarEventType.ORDER, order);
        return order;
    }

    @Override
    public boolean cancelOrder(String originOrderId) {
        if (!isConnected()) {
            throw new IllegalStateException("网关未连线");
        }
        log.info("[{}] 网关收到撤单请求", gd.getGatewayId());
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        if (!orderMap.containsKey(originOrderId)) {
            return true;
        }
        Order order = orderMap.get(originOrderId);
        String symbol = order.contract().symbol();
        parameters.put("symbol", symbol);
        parameters.put("origClientOrderId", originOrderId);
        futuresClient.account().cancelOrder(parameters);
        return true;
    }


    @Override
    public boolean getAuthErrorFlag() {
        return false;
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


}
