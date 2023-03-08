package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.io.IOUtils;


public class Utils {
  public static String loadSchemaFileAsString(String filePath) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }
}
