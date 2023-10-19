package org.dromara.northstar.gateway.binance;

import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.common.model.GatewayDescription;
import org.dromara.northstar.gateway.Gateway;
import org.dromara.northstar.gateway.GatewayFactory;
import org.dromara.northstar.gateway.IMarketCenter;

import com.alibaba.fastjson2.JSON;

public class BinanceGatewayFactory implements GatewayFactory {

    private FastEventEngine fastEventEngine;

    private IMarketCenter mktCenter;

    private BinanceDataServiceManager dataMgr;

    public BinanceGatewayFactory(FastEventEngine fastEventEngine, IMarketCenter mktCenter, BinanceDataServiceManager dataMgr) {
        this.fastEventEngine = fastEventEngine;
        this.mktCenter = mktCenter;
        this.dataMgr = dataMgr;
    }

    @Override
    public Gateway newInstance(GatewayDescription gatewayDescription) {
        BinanceGatewaySettings settings = JSON.parseObject(JSON.toJSONString(gatewayDescription.getSettings()), BinanceGatewaySettings.class);
        gatewayDescription.setSettings(settings);
        new BinanceContractProvider(settings, mktCenter).loadContractOptions();
        return new BinanceMarketGatewayAdapter(fastEventEngine, gatewayDescription, mktCenter);
    }

}
