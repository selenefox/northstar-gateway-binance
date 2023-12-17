package org.dromara.northstar.gateway.binance;

import org.dromara.northstar.common.GatewaySettings;
import org.dromara.northstar.common.constant.FieldType;
import org.dromara.northstar.common.model.DynamicParams;
import org.dromara.northstar.common.model.Setting;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BinanceGatewaySettings extends DynamicParams implements GatewaySettings {

    @Setting(label = "API Key", order = 10, type = FieldType.TEXT)
    private String apiKey;

    @Setting(label = "Secret Key", order = 20, type = FieldType.TEXT)
    private String secretKey;

    @Setting(label = "账户类型", order = 30, type = FieldType.SELECT, options = {"实盘账户", "模拟账户"}, optionsVal = {"true", "false"})
    private boolean accountType;
}
