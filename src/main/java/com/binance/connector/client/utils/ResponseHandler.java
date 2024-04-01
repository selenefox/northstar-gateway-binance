package com.binance.connector.client.utils;

import com.binance.connector.client.exceptions.BinanceClientException;
import com.binance.connector.client.exceptions.BinanceConnectorException;
import com.binance.connector.client.exceptions.BinanceServerException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
public final class ResponseHandler {
    private static final OkHttpClient client = HttpClientSingleton.getHttpClient();
    private static final int HTTP_STATUS_CODE_400 = 400;
    private static final int HTTP_STATUS_CODE_499 = 499;
    private static final int HTTP_STATUS_CODE_500 = 500;

    private ResponseHandler() {
    }

    public static String handleResponse(Request request, boolean showLimitUsage) {
        try (Response response = client.newCall(request).execute()) {
            if (null == response) {
                throw new BinanceServerException("[ResponseHandler] No response from server");
            }

            String responseAsString = getResponseBodyAsString(response.body());

            if (response.code() >= HTTP_STATUS_CODE_400 && response.code() <= HTTP_STATUS_CODE_499) {
                throw handleErrorResponse(responseAsString, response.code());
            } else if (response.code() >= HTTP_STATUS_CODE_500) {
                throw new BinanceServerException(responseAsString, response.code());
            }

            if (showLimitUsage) {
                return getlimitUsage(response, responseAsString);
            } else {
                return responseAsString;
            }
        } catch (IOException | IllegalStateException e) {
            throw new BinanceConnectorException("[ResponseHandler] OKHTTP Error: " + e.getMessage());
        }
    }

    private static String getlimitUsage(Response response, String resposeBodyAsString) {
        JSONObject json = new JSONObject();
        json.put("x-mbx-used-weight", response.header("x-mbx-used-weight"));
        json.put("x-mbx-used-weight-1m", response.header("x-mbx-used-weight-1m"));
        json.put("data", resposeBodyAsString);
        Integer weight1m = Integer.valueOf(response.header("x-mbx-used-weight-1m"));
        //当前权重值,1m不能超过2400
        if (weight1m >= 2300) {
            try {
                LocalDateTime currentTime = LocalDateTime.now();
                LocalDateTime nextMinute = currentTime.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
                long millisecondsUntilNextMinute = ChronoUnit.MILLIS.between(currentTime, nextMinute);
                log.info("币安API接口1m权重即将达到上限需线程等待到下一分钟:[{}]", millisecondsUntilNextMinute);
                Thread.sleep(millisecondsUntilNextMinute);
            } catch (Exception e) {
                log.error("币安API接口1m权重即将达到上限等待异常", e);
            }
        }
        return json.toString();
    }

    private static BinanceClientException handleErrorResponse(String responseBody, int responseCode) {
        try {
            String errorMsg = JSONParser.getJSONStringValue(responseBody, "msg");
            int errorCode = JSONParser.getJSONIntValue(responseBody, "code");
            return new BinanceClientException(responseBody, errorMsg, responseCode, errorCode);
        } catch (JSONException e) {
            throw new BinanceClientException(responseBody, responseCode);
        }
    }

    private static String getResponseBodyAsString(ResponseBody body) throws IOException {
        if (null != body) {
            return body.string();
        } else {
            return "";
        }
    }
}
