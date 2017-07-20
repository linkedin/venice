package com.linkedin.venice.helix;

import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Serializer used to convert data between RouterClusterConfig Object and JSON string
 */
public class RouterClusterConfigJSONSerializer extends VeniceJsonSerializer<RoutersClusterConfig> {
  public RouterClusterConfigJSONSerializer() {
    super(RoutersClusterConfig.class);
  }
}
