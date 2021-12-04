package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

/**
 * A simple container with builder functions to sugar-coat the code a bit.
 */
public class QueryParams {
  protected final Map<String, String> params;
  private final ObjectMapper mapper = new ObjectMapper();

  public QueryParams(Map<String, String> initialParams) {
    this.params = initialParams;
  }

  public QueryParams() {
    this.params = new HashMap<>();
  }

  public QueryParams add(String name, Integer value) {
    return add(name, Integer.toString(value));
  }

  public QueryParams add(String name, Long value) {
    return add(name, Long.toString(value));
  }

  public QueryParams add(String name, Boolean value) {
    return add(name, Boolean.toString(value));
  }

  public QueryParams add(String name, String value) {
    params.put(name, value);
    return this;
  }

  public <TYPE> QueryParams add(String name, Optional<TYPE> optionalValue) {
    optionalValue.ifPresent(o -> this.add(name, o.toString()));
    return this;
  }

  public List<NameValuePair> getNameValuePairs() {
    return params.entrySet().stream()
        .map(entry -> new BasicNameValuePair(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  public List<NameValuePair> getAbbreviatedNameValuePairs() {
    return params.entrySet().stream()
        .map(entry -> new BasicNameValuePair(entry.getKey(), StringUtils.abbreviate(entry.getValue(), 500)))
        .collect(Collectors.toList());
  }

  public QueryParams putStringMap(String name, Map<String, String> value) {
    try {
      return add(name, mapper.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      throw new VeniceException(e.getMessage());
    }
  }

  public Optional<Map<String, String>> getStringMap(String name) {
    if (!params.containsKey(name)) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(mapper.readValue(params.get(name), Map.class));
      } catch (IOException e) {
        throw new VeniceException(e.getMessage());
      }
    }
  }

  @Override
  public String toString() {
    return params.toString();
  }
}
