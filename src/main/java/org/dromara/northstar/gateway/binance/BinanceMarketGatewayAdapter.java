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
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.gateway.Contract;
import org.dromara.northstar.gateway.GatewayAbstract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.MarketGateway;
import org.dromara.northstar.gateway.TradeGateway;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.ContractField;

@Slf4j
public class BinanceMarketGatewayAdapter extends GatewayAbstract implements MarketGateway, TradeGateway {

    private UMWebsocketClientImpl client;

    private ScheduledExecutorService scheduleExec = Executors.newScheduledThreadPool(0);

    private FastEventEngine feEngine;

    private IMarketCenter mktCenter;

    private GatewayDescription gd;

    protected ConnectionState connState = ConnectionState.DISCONNECTED;

    private Map<Identifier, CoreField.TickField> preTickMap = new HashMap<>();

    private List<Integer> streamIdList;

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
        String format = LocalDate.now().format(DateTimeConstant.D_FORMAT_INT_FORMATTER);

        int streamId = client.symbolTicker(symbol, ((event) -> {
            System.out.println(event);
            Map map = JSON.parseObject(event, Map.class);
            try {
                ContractField c = contract.contractField();
                CoreField.TickField.Builder tickBuilder = CoreField.TickField.newBuilder();
                tickBuilder.setUnifiedSymbol(c.getUnifiedSymbol());
                tickBuilder.setGatewayId(gatewayId);
                tickBuilder.setTradingDay(format);
                tickBuilder.setActionDay(format);
                tickBuilder.setActionTime("095900500");
                tickBuilder.setActionTimestamp((Long) map.get("E"));
                tickBuilder.setStatus(TickType.NORMAL_TICK.getCode());
                tickBuilder.setLastPrice(Double.valueOf(String.valueOf(map.get("c"))));
                tickBuilder.setAvgPrice(Double.valueOf(String.valueOf(map.get("w"))));
                tickBuilder.setHighPrice(Double.valueOf(String.valueOf(map.get("h"))));
                tickBuilder.setLowPrice(Double.valueOf(String.valueOf(map.get("l"))));
                tickBuilder.setOpenPrice(Double.valueOf(String.valueOf(map.get("o"))));
                double volume = Double.parseDouble((String) map.get("v"));
                double turnover = Double.parseDouble((String) map.get("q"));

                tickBuilder.setVolume((long) volume);
                tickBuilder.setTurnover((long) turnover);

                tickBuilder.setChannelType(ChannelType.BIAN.toString());
                CoreField.TickField tick = tickBuilder.build();
                preTickMap.put(contract.identifier(), tick);
                feEngine.emitEvent(NorthstarEventType.TICK, tick);

            } catch (Throwable t) {
                log.error("{} OnRtnDepthMarketData Exception", logInfo, t);
            }
        }));
        streamIdList.add(streamId);
        return true;
    }

    @Override
    public boolean unsubscribe(ContractField contract) {
        Iterator<Integer> iterator = streamIdList.iterator();
        while (iterator.hasNext()) {
            client.closeConnection(iterator.next());
            iterator.remove();
        }
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public boolean isActive() {
        // TODO Auto-generated method stub
        return false;
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
