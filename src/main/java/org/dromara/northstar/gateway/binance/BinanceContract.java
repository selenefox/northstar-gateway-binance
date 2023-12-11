package org.dromara.northstar.gateway.binance;

import com.alibaba.fastjson2.JSONObject;

import org.dromara.northstar.common.IDataSource;
import org.dromara.northstar.common.constant.ChannelType;
import org.dromara.northstar.common.model.Identifier;
import org.dromara.northstar.common.model.core.Contract;
import org.dromara.northstar.common.model.core.ContractDefinition;
import org.dromara.northstar.gateway.Instrument;

import xyz.redtorch.pb.CoreEnum.CurrencyEnum;
import xyz.redtorch.pb.CoreEnum.ExchangeEnum;
import xyz.redtorch.pb.CoreEnum.ProductClassEnum;


public class BinanceContract implements Instrument {

    private JSONObject json;

    private ContractDefinition contractDef;

    private IDataSource dataSrc;

    public BinanceContract(JSONObject json, IDataSource dataSrc) {
        this.json = json;
        this.dataSrc = dataSrc;
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
    public ChannelType channelType() {
        return ChannelType.BIAN;
    }

    @Override
    public void setContractDefinition(ContractDefinition contractDef) {
        this.contractDef = contractDef;
    }

    @Override
    public IDataSource dataSource() {
        return dataSrc;
    }

    /**
     * 该合约信息细节还待斟酌
     */
    @Override
    public Contract contract() {

        return Contract.builder()
                .gatewayId(ChannelType.BIAN.toString())
                .symbol(name())
                .name(name())
                .fullName(name())
                .unifiedSymbol(String.format("%s@%s@%s", name(), exchange(), productClass()))
                .currency(CurrencyEnum.USD)
                .exchange(exchange())
                .productClass(productClass())
                .priceTick(json.getDoubleValue("pricePrecision"))
                .pricePrecision(json.getIntValue("pricePrecision"))
                .quantityPrecision(json.getIntValue("quantityPrecision"))
                .multiplier(1 / Math.pow(10, json.getIntValue("quantityPrecision")))
                .contractId(identifier().value())
                .longMarginRatio(json.getDoubleValue("longMarginRatio"))
                .shortMarginRatio(json.getDoubleValue("shortMarginRatio"))
                .channelType(ChannelType.BIAN)
                .build();
    }

}
