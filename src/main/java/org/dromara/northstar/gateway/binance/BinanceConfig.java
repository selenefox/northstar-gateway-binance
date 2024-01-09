package org.dromara.northstar.gateway.binance;

import org.dromara.northstar.common.event.FastEventEngine;
import org.dromara.northstar.gateway.IMarketCenter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * @author 李嘉豪
 * @version 1.0
 * @date 2023/9/21 13:14
 */
@Slf4j
@Configuration
public class BinanceConfig {
    static {
        log.info("=====================================================");
        log.info("                 加载gateway-binance                  ");
        log.info("=====================================================");
    }

    @Bean
    BinanceDataServiceManager binanceDataServiceManager() {
        return new BinanceDataServiceManager();
    }


    @Bean
    BinanceGatewayFactory binanceGatewayFactory(FastEventEngine feEngine, IMarketCenter mktCenter,
                                                @Qualifier("binanceDataServiceManager") BinanceDataServiceManager dsMgr) {
        return new BinanceGatewayFactory(feEngine, mktCenter, dsMgr);
    }

    @Bean
    BinanceGatewaySettings binanceGatewaySettings() {
        return new BinanceGatewaySettings();
    }

}
