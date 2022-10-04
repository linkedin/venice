package com.linkedin.alpini.base.misc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;


/**
 * A VERY simple JSON mapper.  This class will take a string of simple JSON and convert it
 * into a Map.
 *
 * Created by zpolicze on 10/29/14.
 */
public enum SimpleJsonMapper {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static Supplier<ObjectMapper> _objectMapper = new Supplier<ObjectMapper>() {
    @Override
    public synchronized ObjectMapper get() {
      if (this == _objectMapper) {
        ObjectMapper objectMapper = new ObjectMapper();
        _objectMapper = () -> objectMapper;
      }
      return _objectMapper.get();
    }
  };

  public static ObjectMapper getObjectMapper() {
    return _objectMapper.get();
  }

  /**
   * Converts valid JSON document into a hash map of Strings.
   *
   * @param json     a valid json string to be mapped.
   * @return         the inputted json string as a map.
   *
   * @throws java.lang.Exception when parameter json is not a valid json string.
   */
  public static Map<String, Object> mapJSON(String json) throws Exception {
    ObjectMapper mapper = getObjectMapper();
    Map<String, Object> map = mapper.readValue(json, new TypeReference<HashMap<String, Object>>() {
    });

    // Iterate through the fields and recursively call mapJSON so that nested JSON objects are caught and converted to a
    // map
    for (Map.Entry<String, Object> entry: new ArrayList<>(map.entrySet())) {
      if (entry.getValue() instanceof String) {
        try {
          Map<String, Object> mapToInsert = mapJSON((String) entry.getValue());
          map.put(entry.getKey(), mapToInsert);
        } catch (Exception e) {
          continue;
        }
      }
    }

    return map;
  }
}
