package com.linkedin.venice.fastclient;

import com.linkedin.venice.read.RequestType;
import java.util.List;
import java.util.Map;


/**
 * Request context for CountByValue aggregation operations.
 * This context carries the parameters and state needed for server-side aggregation.
 */
public class CountByValueRequestContext<K> extends MultiKeyRequestContext<K, Map<String, Map<Object, Integer>>> {
  private final List<String> fieldNames;
  private final int topK;
  private Map<String, Map<Object, Integer>> result;

  public CountByValueRequestContext(int keysCount, List<String> fieldNames, int topK) {
    super(keysCount, false); // not allowing partial success for simplicity
    this.fieldNames = fieldNames;
    this.topK = topK;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COUNT_BY_VALUE;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public int getTopK() {
    return topK;
  }

  public Map<String, Map<Object, Integer>> getResult() {
    return result;
  }

  public void setResult(Map<String, Map<Object, Integer>> result) {
    this.result = result;
  }
}
