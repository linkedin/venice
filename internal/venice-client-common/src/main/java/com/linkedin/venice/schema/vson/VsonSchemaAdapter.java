package com.linkedin.venice.schema.vson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Deprecated
public class VsonSchemaAdapter extends AbstractVsonSchemaAdapter<Object> {
  public static Object parse(String vsonSchemaStr) {
    VsonSchemaAdapter adapter = new VsonSchemaAdapter(vsonSchemaStr);
    return adapter.fromVsonObjects();
  }

  private VsonSchemaAdapter(String vsonSchemaStr) {
    super(vsonSchemaStr);
  }

  @Override
  protected Object readMap(Map<String, Object> vsonMap) {
    Map<String, Object> newM = new LinkedHashMap<>(vsonMap.size());
    List<String> keys = new ArrayList<>((vsonMap.keySet()));
    Collections.sort(keys);
    for (String key: keys) {
      newM.put(key, fromVsonObjects(vsonMap.get(key)));
    }

    return newM;
  }

  @Override
  Object readList(List<Object> vsonList) {
    return Arrays.asList(fromVsonObjects(vsonList.get(0)));
  }

  @Override
  Object readPrimitive(String vsonString) {
    return VsonTypes.fromDisplay(vsonString);
  }
}
