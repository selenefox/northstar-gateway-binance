package org.dromara.northstar.gateway.binance;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.impl.UMFuturesClientImpl;
import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.exception.TradeException;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.gateway.Contract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.TradeGateway;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;
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
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreEnum;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.CancelOrderReqField;
import xyz.redtorch.pb.CoreField.SubmitOrderReqField;

@Slf4j
public class BinanceTradeGatewayLocal implements TradeGateway {

    protected FastEventEngine feEngine;

    @Getter
    private boolean connected;

    private GatewayDescription gd;

    private ConnectionState connState = ConnectionState.DISCONNECTED;

    private Timer statusReportTimer;

    private IMarketCenter mktCenter;

    private BinanceGatewaySettings settings;

    private UMFuturesClientImpl futuresClient;

    protected Map<String, CoreField.OrderField> orderMap = new HashMap<>();

    private UMWebsocketClientImpl websocketClient = new UMWebsocketClientImpl();

    private List<Integer> streamIdList = new ArrayList<>();


    public BinanceTradeGatewayLocal(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        this.settings = (BinanceGatewaySettings) gd.getSettings();
        this.futuresClient = new UMFuturesClientImpl(settings.getApiKey(), settings.getSecretKey());
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
                String result;
                try {
                    result = futuresClient.account().accountInformation(new LinkedHashMap<>());
                }catch (Exception e){
                    e.getStackTrace();
                    result = futuresClient.account().accountInformation(new LinkedHashMap<>());
                }
                JSONObject accountInformation = JSON.parseObject(result);
                //账户事件
                getAccountField(accountInformation);
                JSONArray positions = jsonObject.get().getJSONArray("positions");
                List<JSONObject> positionList = positions.stream().map(item -> (JSONObject) item).filter(item -> item.getDouble("positionAmt") > 0).collect(Collectors.toList());

                for (JSONObject position : positionList) {
                    String symbol = position.getString("symbol");
                    String positionSide = position.getString("positionSide");
                    CoreEnum.PositionDirectionEnum posDir = "LONG".equals(positionSide) ? CoreEnum.PositionDirectionEnum.PD_Long : CoreEnum.PositionDirectionEnum.PD_Short;

                    Contract contract = mktCenter.getContract(ChannelType.BIAN, symbol);
                    CoreField.ContractField contracted = contract.contractField();
                    //持仓数量按照最小交易精度转换
                    int positionAmt = Double.valueOf(position.getDouble("positionAmt") / contracted.getMultiplier()).intValue();

                    CoreField.PositionField.Builder positionBuilder = CoreField.PositionField.newBuilder()
                            .setPositionId(contracted.getUnifiedSymbol() + "@" + posDir)
                            .setGatewayId(gd.getGatewayId())
                            .setPositionDirection(posDir)
                            .setPosition(positionAmt)
                            .setTdPosition(positionAmt)
                            .setYdPosition(positionAmt)
                            .setContract(contracted)
                            .setLastPrice(position.getDouble("entryPrice"))
                            .setPrice(position.getDouble("entryPrice"))
                            .setOpenPrice(position.getDouble("entryPrice"))
                            .setOpenPositionProfit(position.getDouble("unrealizedProfit"))
                            .setOpenPriceDiff(position.getDouble("unrealizedProfit"))
                            .setPriceDiff(position.getDouble("unrealizedProfit"))
                            .setPositionProfit(position.getDouble("unrealizedProfit"))
                            .setUseMargin(position.getDouble("positionInitialMargin"))
                            .setExchangeMargin(position.getDouble("positionInitialMargin"));
                    if (positionBuilder.getUseMargin() != 0) {
                        positionBuilder.setPositionProfitRatio(positionBuilder.getPositionProfit() / positionBuilder.getUseMargin());
                        positionBuilder.setOpenPositionProfitRatio(positionBuilder.getOpenPositionProfit() / positionBuilder.getUseMargin());
                    }
                    feEngine.emitEvent(NorthstarEventType.POSITION, positionBuilder.build());
                }
            }

        }, 5000, 2000);
    }

    private void currentAllOpenOrders() {
        String result = futuresClient.account().currentAllOpenOrders(new LinkedHashMap<>());
        List<JSONObject> openOrderList = JSON.parseArray(result).stream().map(item -> (JSONObject) item).collect(Collectors.toList());
        //维护订单ID和symbol的map
        for (JSONObject order : openOrderList) {
            CoreField.ContractField contractField = mktCenter.getContract(ChannelType.BIAN, order.getString("symbol")).contractField();
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
            CoreField.OrderField.Builder orderBuilder = getOrderBuilder(dBuy, order, contractField, offsetFlag);
            orderMap.put(order.getString("clientOrderId"), orderBuilder.build());
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
        CoreField.AccountField accountBuilder = CoreField.AccountField.newBuilder()
                .setAccountId(gd.getGatewayId())
                .setAvailable(jsonObject.getDouble("availableBalance"))
                .setBalance(jsonObject.getDouble("totalCrossWalletBalance"))
                .setCloseProfit(jsonObject.getDouble("totalUnrealizedProfit"))
                //TODO，ETH/BTC期货合约将按照BUSD手续费表计。这里币安返回的feeTier是手续费等级,0=0.0200%/0.0500%(USDT-Maker / Taker),暂时写死后续处理
                .setCommission(Double.valueOf(0.0002))
                .setGatewayId(gd.getGatewayId())
                .setCurrency(CoreEnum.CurrencyEnum.USD)
                .setName("BIAN")
                .setMargin(jsonObject.getDouble("totalInitialMargin"))
                .setPositionProfit(jsonObject.getDouble("totalCrossUnPnl"))
                .build();
        feEngine.emitEvent(NorthstarEventType.ACCOUNT, accountBuilder);
    }

    //Balance和Position更新推送
    private JSONObject accountUpdate(JSONObject eventJson) {
        return getAccountInformation();
    }

    //订单/交易 更新推送
    private JSONObject orderTradeUpdate(JSONObject json) {
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
    public String submitOrder(SubmitOrderReqField submitOrderReq) throws TradeException {
        if (!isConnected()) {
            throw new IllegalStateException("网关未连线");
        }
        log.info("[{}] 网关收到下单请求,参数:[{}]", gd.getGatewayId(), submitOrderReq);
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();

        CoreField.ContractField contract = submitOrderReq.getContract();
        CoreEnum.DirectionEnum direction = submitOrderReq.getDirection();
        CoreEnum.OffsetFlagEnum offsetFlag = submitOrderReq.getOffsetFlag();
        CoreEnum.TimeConditionEnum timeCondition = submitOrderReq.getTimeCondition();
        //数量 * 最小交易数量
        double quantity = submitOrderReq.getVolume() * contract.getMultiplier();
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
        //开仓
        if (CoreEnum.OffsetFlagEnum.OF_Open.getNumber() == offsetFlag.getNumber()) {
            side = "BUY";
            if (submitOrderReq.getPrice() == 0) {
                type = "MARKET";
            } else {
                type = "LIMIT";
                parameters.put("timeInForce", timeInForce);
                parameters.put("price", submitOrderReq.getPrice());
            }
            //持仓方向 或
            if (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) {
                positionSide = "LONG";
            } else {
                positionSide = "SHORT";
            }
            parameters.put("positionSide", positionSide);
        } else {
            //平仓
            side = "SELL";
            if (submitOrderReq.getPrice() == 0) {
                //止盈市价单
                type = "TAKE_PROFIT_MARKET";
            } else {
                //止盈限价单
                type = "TAKE_PROFIT";
                parameters.put("timeInForce", timeInForce);
                parameters.put("price", submitOrderReq.getPrice());
            }
            parameters.put("stopPrice", submitOrderReq.getPrice());
        }

        //订单种类,市价单不传价格

        parameters.put("symbol", contract.getSymbol());
        parameters.put("side", side);
        parameters.put("type", type);
        parameters.put("quantity", quantity);
        parameters.put("newClientOrderId", submitOrderReq.getOriginOrderId());

        //向币安提交订单
        String s = futuresClient.account().newOrder(parameters);
        log.info("[{}] 网关收到下单返回,响应:[{}]", gd.getGatewayId(), s);

        JSONObject orderJson = JSON.parseObject(s);
        //查询全部挂单
        currentAllOpenOrders();
        return submitOrderReq.getOriginOrderId();
    }

    @NotNull
    private CoreField.OrderField.Builder getOrderBuilder(CoreEnum.DirectionEnum directionEnum, JSONObject orderJson, CoreField.ContractField contract, CoreEnum.OffsetFlagEnum offsetFlag) {
        CoreField.OrderField.Builder orderBuilder = CoreField.OrderField.newBuilder();

        //数量 * 最小交易数量
        int origQty = (int) Math.round(orderJson.getDouble("origQty") * Math.pow(10, contract.getQuantityPrecision()));
        int executedQty = (int) Math.round(orderJson.getDouble("executedQty") * Math.pow(10, contract.getQuantityPrecision()));
        orderBuilder.setOrderId(orderJson.getString("clientOrderId"));
        orderBuilder.setOriginOrderId(orderJson.getString("clientOrderId"));
        orderBuilder.setAccountId(settings.getApiKey());
        orderBuilder.setDirection(directionEnum);
        orderBuilder.setOffsetFlag(offsetFlag);
        orderBuilder.setOrderStatus(CoreEnum.OrderStatusEnum.OS_AllTraded);
        orderBuilder.setPrice(orderJson.getDouble("price"));
        orderBuilder.setTotalVolume(origQty);
        orderBuilder.setTradedVolume(executedQty);
        orderBuilder.setContract(contract);
        orderBuilder.setGatewayId(gd.getGatewayId());
        orderBuilder.setStatusMsg("已报单").setOrderStatus(CoreEnum.OrderStatusEnum.OS_Unknown);
        orderBuilder.setCancelTime(LocalTime.now().format(DateTimeConstant.T_FORMAT_FORMATTER));
        orderBuilder.setUpdateTime(LocalTime.now().format(DateTimeConstant.T_FORMAT_FORMATTER));
        feEngine.emitEvent(NorthstarEventType.ORDER, orderBuilder.build());
        return orderBuilder;
    }

    @Override
    public boolean cancelOrder(CancelOrderReqField cancelOrderReq) {
        if (!isConnected()) {
            throw new IllegalStateException("网关未连线");
        }
        log.info("[{}] 网关收到撤单请求", gd.getGatewayId());
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
        CoreField.OrderField order = orderMap.get(cancelOrderReq.getOriginOrderId());
        String symbol = order.getContract().getSymbol();
        parameters.put("symbol", symbol);
        parameters.put("origClientOrderId", cancelOrderReq.getOriginOrderId());
        futuresClient.account().cancelOrder(parameters);
        CoreField.OrderField.Builder builder = order.toBuilder()
                .setStatusMsg("已撤单")
                .setOrderStatus(CoreEnum.OrderStatusEnum.OS_Canceled)
                .setCancelTime(LocalTime.now().format(DateTimeConstant.T_FORMAT_FORMATTER))
                .setUpdateTime(LocalTime.now().format(DateTimeConstant.T_FORMAT_FORMATTER));

        feEngine.emitEvent(NorthstarEventType.ORDER, builder.build());
        log.info("[{}] 订单反馈：{} {} {} {} {}", order.getGatewayId(), order.getOrderDate(), order.getUpdateTime(), order.getOriginOrderId(), order.getOrderStatus(), order.getStatusMsg());
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
