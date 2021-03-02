package com.linkedin.venice.router.utils;

import com.linkedin.venice.router.api.VenicePathParserHelper;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.AttributeKey;


public class VeniceRouterUtils {
  public static final String METHOD_GET = HttpMethod.GET.name();
  public static final String METHOD_POST = HttpMethod.POST.name();
  public static final AttributeKey<VenicePathParserHelper> PATHPARSER_ATTRIBUTE_KEY =
      AttributeKey.valueOf("PATHPARSER_ATTRIBUTE_KEY");

  public static boolean isHttpGet(String methodName) {
    return methodName.equalsIgnoreCase(METHOD_GET);
  }

  public static boolean isHttpPost(String methodName) {
    return methodName.equalsIgnoreCase(METHOD_POST);
  }
}
