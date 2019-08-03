package dev.jozefowicz.stacjait.mynotes.common;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.nonNull;

public final class APIGatewayProxyResponseEventBuilder {

    private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";

    public final static APIGatewayProxyResponseEvent response(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        Map<String, String> headers = new HashMap<>();
        headers.put(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        headers.put(ACCESS_CONTROL_ALLOW_CREDENTIALS, Boolean.TRUE.toString());
        response.setHeaders(headers);
        if (nonNull(body)) {
            response.setBody(body);
        }
        return response;
    }

}
