package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.commons.collections.map.HashedMap;
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
}
