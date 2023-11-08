package org.dromara.northstar.gateway.binance;

import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.gateway.Instrument;
import org.dromara.northstar.gateway.TradeTimeDefinition;
import org.dromara.northstar.gateway.model.ContractDefinition;
import org.dromara.northstar.gateway.time.GenericTradeTime;

import com.alibaba.fastjson2.JSONObject;

import xyz.redtorch.pb.CoreEnum.CurrencyEnum;
import xyz.redtorch.pb.CoreEnum.ExchangeEnum;
import xyz.redtorch.pb.CoreEnum.ProductClassEnum;
import xyz.redtorch.pb.CoreField.ContractField;


public class BinanceContract implements Instrument {

    private JSONObject json;

    private ContractDefinition contractDef;

    public BinanceContract(JSONObject json) {
        this.json = json;
    }

    @Override
    public String name() {
        return json.getString("symbol");
    }

    @Override
    public Identifier identifier() {
        return Identifier.of(String.format("%s@%s@%s@%s", name(), exchange(), productClass(), channelType()));
    }

    @Override
    public ProductClassEnum productClass() {
        return ProductClassEnum.SWAP;
    }

    @Override
    public ExchangeEnum exchange() {
        return ExchangeEnum.BINANCE;
    }

    @Override
    public TradeTimeDefinition tradeTimeDefinition() {
        return new GenericTradeTime();
    }

    @Override
    public ChannelType channelType() {
        return ChannelType.BIAN;
    }

    @Override
    public void setContractDefinition(ContractDefinition contractDef) {
        this.contractDef = contractDef;
    }

    /**
     * 该合约信息细节还待斟酌
     */
    @Override
    public ContractField contractField() {
        return ContractField.newBuilder()
                .setGatewayId(ChannelType.BIAN.toString())
                .setSymbol(name())
                .setName(name())
                .setFullName(name())
                .setUnifiedSymbol(String.format("%s@%s@%s", name(), exchange(), productClass()))
                .setCurrency(CurrencyEnum.USD)
                .setExchange(exchange())
                .setProductClass(productClass())
                .setPriceTick(json.getDoubleValue("pricePrecision"))
                .setPricePrecision(json.getIntValue("pricePrecision"))
                .setQuantityPrecision(json.getIntValue("quantityPrecision"))
                .setMultiplier(1 / Math.pow(10, json.getIntValue("quantityPrecision")))
                .setContractId(identifier().value())
                .setLongMarginRatio(json.getDoubleValue("longMarginRatio"))
                .setShortMarginRatio(json.getDoubleValue("shortMarginRatio"))
                .setChannelType(ChannelType.BIAN.toString())
                .build();
    }

}
