package org.dromara.northstar.gateway.binance;

import org.apache.commons.lang3.StringUtils;
import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.constant.DateTimeConstant;
import org.dromara.northstar.gateway.GatewayMetaProvider;
import org.dromara.northstar.gateway.IMarketCenter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import xyz.redtorch.pb.CoreEnum;

/**
 * @author 李嘉豪
 * @version 1.0
 * @date 2023/9/20 16:59
 */

@Slf4j
@Order(0)   // 加载顺序需要显式声明，否则会最后才被加载，从而导致加载网关与模组时报异常
@Component
public class BinanceLoader implements CommandLineRunner {

    @Autowired
    private IMarketCenter mktCenter;

    @Autowired
    private GatewayMetaProvider gatewayMetaProvider;

    @Resource(name = "binanceDataServiceManager")
    private BinanceDataServiceManager dsMgr;

    @Autowired
    private BinanceGatewayFactory binanceGatewayFactory;

    @Autowired
    private BinanceContractProvider binanceContractProvider;

    @Override
    public void run(String... args) throws Exception {
        gatewayMetaProvider.add(ChannelType.BIAN, new BinanceGatewaySettings(), binanceGatewayFactory, dsMgr);
        // 加载BIAN增加合约定义
        mktCenter.addDefinitions(binanceContractProvider.get());

        mktCenter.loadContractGroup(ChannelType.BIAN);
    }
}
