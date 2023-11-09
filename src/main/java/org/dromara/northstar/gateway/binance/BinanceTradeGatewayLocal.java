package org.dromara.northstar.gateway.binance;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.binance.connector.client.impl.UMFuturesClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.exception.TradeException;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.data.ISimAccountRepository;
import org.dromara.northstar.gateway.Contract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.TradeGateway;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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

    private ISimAccountRepository simAccountRepo;

    private Timer statusReportTimer;

    private IMarketCenter mktCenter;

    private UMFuturesClientImpl futuresClient;

    private Map<String,String> orderIdBySymbol;

    public BinanceTradeGatewayLocal(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        BinanceGatewaySettings settings = (BinanceGatewaySettings) gd.getSettings();
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
        String result = futuresClient.account().accountInformation(new LinkedHashMap<>());
        //更新合约多头空头保证金率，添加持仓回报事件
        JSONObject jsonObject = JSON.parseObject(result);
        JSONArray positions = jsonObject.getJSONArray("positions");
        List<JSONObject> positionList = positions.stream().map(item -> (JSONObject) item).filter(item -> item.getDouble("positionAmt") > 0).collect(Collectors.toList());
        statusReportTimer = new Timer("BinanceGatewayTimelyReport", true);
        statusReportTimer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
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
                for (JSONObject position : positionList) {
                    String symbol = position.getString("symbol");
                    String positionSide = position.getString("positionSide");
                    String positionId = symbol + "@" + ChannelType.BIAN + "@" + CoreEnum.ProductClassEnum.SWAP + "@" + positionSide + "@" + positionSide;
                    Contract contract = mktCenter.getContract(ChannelType.BIAN, symbol);
                    CoreField.ContractField contracted = contract.contractField();
                    //持仓数量按照最小交易精度转换
                    int positionAmt = Double.valueOf(position.getIntValue("positionAmt") / contracted.getMultiplier()).intValue();

                    CoreField.PositionField.Builder positionBuilder = CoreField.PositionField.newBuilder()
                            .setPositionId(positionId)
                            .setAccountId(gd.getGatewayId())
                            .setGatewayId(gd.getGatewayId())
                            .setPositionDirection("SHORT".equals(positionSide) ? CoreEnum.PositionDirectionEnum.PD_Short : CoreEnum.PositionDirectionEnum.PD_Long)
                            .setPosition(positionAmt)
                            .setPrice(position.getDouble("entryPrice"))
                            .setPositionProfit(position.getDouble("unrealizedProfit"))
                            .setUseMargin(position.getDouble("positionInitialMargin"));
                    if (positionBuilder.getUseMargin() != 0) {
                        positionBuilder.setPositionProfitRatio(positionBuilder.getPositionProfit() / positionBuilder.getUseMargin());
                        positionBuilder.setOpenPositionProfitRatio(positionBuilder.getOpenPositionProfit() / positionBuilder.getUseMargin());
                    }
                    feEngine.emitEvent(NorthstarEventType.POSITION, positionBuilder.build());
                }
            }

        }, 5000, 2000);
    }

    @Override
    public void disconnect() {
        log.debug("[{}] 模拟网关断开", gd.getGatewayId());
        connected = false;
        connState = ConnectionState.DISCONNECTED;
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
        log.info("[{}] 网关收到下单请求,参数:[{}]", gd.getGatewayId(),submitOrderReq);
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
        //开仓
        if (CoreEnum.OffsetFlagEnum.OF_Open.getNumber() == offsetFlag.getNumber()) {
            side = "BUY";
        } else {
            side = "SELL";
        }
        //持仓方向 或
        if (CoreEnum.DirectionEnum.D_Buy.getNumber() == direction.getNumber()) {
            positionSide = "LONG";
        } else {
            positionSide = "SHORT";
        }
        //有效方式
        switch (timeCondition) {
            case TC_IOC -> timeInForce = "IOC";
            case TC_GTD -> timeInForce = "GTD";
            case TC_GTC -> timeInForce = "GTC";
            default -> timeInForce = "GTC";
        }
        //订单种类,市价单不传价格
        if (submitOrderReq.getPrice() == 0) {
            type = "MARKET";
        } else {
            type = "LIMIT";
            parameters.put("price", submitOrderReq.getPrice());
        }

        parameters.put("symbol", contract.getSymbol());
        parameters.put("side", side);
        parameters.put("positionSide", positionSide);
        parameters.put("type", type);
        parameters.put("timeInForce", timeInForce);
        parameters.put("quantity", quantity);
        parameters.put("newClientOrderId", submitOrderReq.getOriginOrderId());
        String s = futuresClient.account().newOrder(parameters);
        orderIdBySymbol.put(submitOrderReq.getOriginOrderId(), contract.getSymbol())
        return submitOrderReq.getOriginOrderId();
    }

    @Override
    public boolean cancelOrder(CancelOrderReqField cancelOrderReq) {
        if (!isConnected()) {
            throw new IllegalStateException("网关未连线");
        }
        log.info("[{}] 网关收到撤单请求", gd.getGatewayId());
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();

        parameters.put("symbol", orderIdBySymbol.get(cancelOrderReq.getOrderId()));
        parameters.put("orderId", cancelOrderReq.getOrderId());
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
