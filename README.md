# northstar-gateway-binance

northstar盈富量化平台币安网关接口实现

使用时，需要依赖 northstar 主程序进行加载，详情参考 https://gitee.com/dromara/northstar

## 如何使用


1. **如何使用币安网关**
   1. **创建BIAN网关行情：** 网关配置中的API Key、Secret Key需要在币安官网申请，币安官网也提供了一套完整的模拟交易环境。
   2. **使用时：** 需要依赖 northstar 主程序进行加载，**把 northstar-gateway-binance-<版本号>.jar 与主程序jar包置于同一目录下，然后启动主程序**。
   3. **网关配置：** 有一个选项账户配置：【实盘账户】、【模拟账户】。
   4. **交易操作：** 在Northstar的用户界面上，用户可以看到与币安网关对接后的实时数字货币行情。选择相应的交易对和交易量后，即可进行下单操作。账户类型选择：【BIAN】，网关配置和创建BIAN网关行情一样，需要API Key、Secret Key，账户类型需要和行情网关保持一致。
   5. **交易操作：** 在Northstar的用户界面上，用户可以看到与币安网关对接后的实时数字货币行情。选择相应的交易对和交易量后，即可进行下单操作。币安官网提供了历史数据，创建PLAYBACK行情，订阅合约选择感兴趣的币种合约，配置回访日期、回放精度、回访速度。
   6. **交易操作：** 在Northstar的用户界面上，用户可以看到与币安网关对接后的实时数字货币行情。选择相应的交易对和交易量后，即可进行下单操作。
   7. **手工交易：** 页签，选择创建的账户和订阅的合约，选择完毕后会自动加载当前账户下，合约的持仓情况、实时k线数据、账户可用资金。
2. **如何进行历史回测**
   1. **历史回测：** 实质上是利用历史数据进行回放。Binance官方提供了历史数据，我们只需创建"PLAYBACK"市场，然后配置网关设置回放精度、回放速度、回放日期和回放合约。
   2. **创建模拟账户：** 选择账户类型为SIM，绑定"PLAYBACK"市场，通过【出入金】为账户设置金额。
   3. **模组：** 创建模组，将其绑定到"PLAYBACK"市场，SIM账户和策略，即可实现对Binance合约的历史回测。
3. **自动化交易**
   1. **全天24小时自动交易：**量化交易最重要的是解放双手，不用一天天盯盘。在币圈这个7*24小时开盘的市场中，人工无法持续盯盘，因此需要自动交易。按照设定好的交易策略自动进行交易，这大大提高了交易效率和准确性，同时减少了人工操作的误差和情绪干扰。
   2. **多个交易合约同时自动化交易：**我们可以创建多个模组，模组和策略之间是多对多的关系，这样可以同时对多个合约进行自动化交易，极大地提高了生产效率，解放了双手，我们只需要关注交易策略即可。
4. **编写交易策略**
   1. **如何编写交易策略：** 在Northstar中编写交易策略只需要掌握基本的Java就可以自行编写各种策略。
   2. **如何使用指标来编写策略：** 通常策略需要指标来判断趋势，在Northstar中基本的指标已经实现。按照【示例策略】中的指引进行简单的配置，即可开始使用指标进行策略开发。
   3. **如何进行策略的验证：** 当我们开发完一个策略后，使用历史回测进行验证是最快捷的方式。我们可以创建多个合约并绑定策略，验证策略是否按照预期执行，也可以找到适合该策略的最佳合约。
5. **注意事项**
   1. **模拟账户：** 实盘账户的API Key和模拟账户不通用，需要单独申请。
   2. **下单数量：** 币安合约中对下单数量是有要求的，举个例子：在BTCUSDT合约下单最小数量为0.003BTC或者等价值的USDT。
   3. **BIAN网关中的精度：** 成交量，下单数量，持仓数量都是按照最小交易精度转换后的，举个例子：在BTCUSDT合约中，下单数量为1，并不是下单了1个BTC而是1*最小交易精度（0.001），成交量，持仓量亦是如此。
   4. **代理：** 币安Api接口需要使用代理，启动项目时添加JVM参数配置代理ip端口
      -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=18081 -Dhttps.proxyHost=127.0.0.1
      -Dhttps.proxyPort=18081
   5. **官网文档：** 请使用前先通读一遍 [【官网文档】](https://www.quantit.tech/) 

