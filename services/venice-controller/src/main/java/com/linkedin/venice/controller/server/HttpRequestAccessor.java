package com.linkedin.venice.controller.server;

import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;

import com.linkedin.venice.authentication.AuthenticationService;
import java.security.cert.X509Certificate;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import spark.Request;


public class HttpRequestAccessor implements AuthenticationService.HttpRequestAccessor {
  private final Request request;

  public HttpRequestAccessor(Request request) {
    this.request = request;
  }

  @Override
  public String getHeader(String headerName) {
    return request.headers(headerName);
  }

  @Override
  public X509Certificate getCertificate() {
    HttpServletRequest rawRequest = request.raw();
    Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    if (certificateObject == null) {
      return null;
    }
    return ((X509Certificate[]) certificateObject)[0];
  }

  @Override
  public String toString() {
    return "Request {" + request.requestMethod() + ", " + request.pathInfo() + "?" + request.queryString()
        + request.headers().stream().map(h -> h + ":" + request.headers(h)).collect(Collectors.joining()) + "}";
  }
}
