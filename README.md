# northstar-gateway-binance

northstar盈富量化平台币安网关接口实现

使用时，需要依赖 northstar 主程序进行加载，详情参考 https://gitee.com/dromara/northstar

币安Api接口需要使用代理，启动项目时添加JVM参数配置代理ip端口
-Dhttp.proxyHost=127.0.0.1
-Dhttp.proxyPort=18081

币安网关中的成交量，下单数量，持仓数量都是按照最小交易精度转换后的，举个例子：在BTCUSDT合约中，下单数量为1，并不是下单了1个BTC而是1*最小交易精度（0.001），成交量，持仓量亦是如此
