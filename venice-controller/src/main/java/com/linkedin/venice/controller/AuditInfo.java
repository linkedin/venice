package com.linkedin.venice.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import spark.Request;


public class AuditInfo {

  private String url;
  private Map<String, String> params;
  private String method;

  public AuditInfo(Request request){
    this.url = request.url();
    this.params = new HashMap<>();
    for (String param : request.queryParams()){
      this.params.put(param, request.queryParams(param));
    }
    this.method = request.requestMethod();
  }

  @Override
  public String toString(){
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("[AUDIT]");
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());
    return joiner.toString();
  }

  public String successString(){
    return toString(true, null);
  }

  public String failureString(String errMsg){
    return toString(false, errMsg);
  }

  private String toString(boolean success, String errMsg){
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("[AUDIT]");
    if (success){
      joiner.add("SUCCESS");
    } else {
      joiner.add("FAILURE: ");
      if (null != errMsg){
        joiner.add(errMsg);
      }
    }
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());
    return joiner.toString();
  }
}
