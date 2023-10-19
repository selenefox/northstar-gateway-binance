package org.dromara.northstar.gateway.binance;

import com.binance.connector.client.impl.UMWebsocketClientImpl;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.ConnectionState;
import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.event.NorthstarEventType;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.gateway.GatewayAbstract;
import org.dromara.northstar.gateway.IMarketCenter;
import org.dromara.northstar.gateway.MarketGateway;
import org.dromara.northstar.gateway.TradeGateway;
import org.dromara.northstar.gateway.contract.GatewayContract;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreField;
import xyz.redtorch.pb.CoreField.ContractField;

@Slf4j
public class BinanceMarketGatewayAdapter extends GatewayAbstract implements MarketGateway, TradeGateway {

    private UMWebsocketClientImpl client;

    private ScheduledExecutorService scheduleExec = Executors.newScheduledThreadPool(0);

    private ScheduledFuture<?> task;

    private long lastActiveTime;

    private FastEventEngine feEngine;

    private IMarketCenter mktCenter;

    private GatewayDescription gd;

    protected ConnectionState connState = ConnectionState.DISCONNECTED;

    public BinanceMarketGatewayAdapter(FastEventEngine feEngine, GatewayDescription gd, IMarketCenter mktCenter) {
        super(gd, mktCenter);
        this.feEngine = feEngine;
        this.gd = gd;
        this.mktCenter = mktCenter;
    }

    @Override
    public void connect() {
        connState = ConnectionState.CONNECTED;
        client = new UMWebsocketClientImpl();

        if (connState == ConnectionState.CONNECTED) {
            return;
        }
        task = scheduleExec.scheduleAtFixedRate(() -> {
            lastActiveTime = System.currentTimeMillis();
            try {
                client.continuousKlineStream("btcusdt", "perpetual", "1m", ((event) -> {
                    System.out.println(event);
                }));
            } catch (Exception e) {
                log.error("模拟行情TICK生成异常", e);
            }
        }, 500, 500, TimeUnit.MILLISECONDS);

        connState = ConnectionState.CONNECTED;
        CompletableFuture.runAsync(() -> feEngine.emitEvent(NorthstarEventType.GATEWAY_READY, gd.getGatewayId()),
                CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS));

    }

    @Override
    public void disconnect() {
        client.closeAllConnections();
        connState = ConnectionState.DISCONNECTED;
    }

    @Override
    public boolean getAuthErrorFlag() {
        return false;
    }

    @Override
    public boolean subscribe(ContractField contract) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean unsubscribe(ContractField contract) {
        // TODO Auto-generated method stub
        return false;
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
