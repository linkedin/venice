package com.linkedin.venice.router.utils;

import io.netty.handler.codec.http.HttpMethod;


public class VeniceRouterUtils {
  public static final String METHOD_GET = HttpMethod.GET.name();
  public static final String METHOD_POST = HttpMethod.POST.name();

  public static boolean isHttpGet(String methodName) {
    return methodName.equalsIgnoreCase(METHOD_GET);
  }

  public static boolean isHttpPost(String methodName) {
    return methodName.equalsIgnoreCase(METHOD_POST);
  }
}
