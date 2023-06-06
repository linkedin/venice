package com.linkedin.venice.authentication.jwt;

import com.linkedin.venice.authentication.ClientAuthenticationProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClientAuthenticationProviderToken implements ClientAuthenticationProvider {
  private static final Logger log = LogManager.getLogger(ClientAuthenticationProviderToken.class);

  private Map<String, String> headers;

  public ClientAuthenticationProviderToken() {
  }

  public static ClientAuthenticationProviderToken TOKEN(String token) {
    ClientAuthenticationProviderToken provider = new ClientAuthenticationProviderToken();
    provider.headers = Collections.singletonMap("Authorization", "Bearer " + token);
    return provider;
  }

  @Override
  public void initialize(VeniceProperties veniceProperties) throws Exception {
    String token = veniceProperties.getString("authentication.token", "");
    if (token.startsWith("file://")) {
      String path = token.substring("file://".length());
      log.info("Reading JWT token from {}", path);
      token = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }
    if (token.isEmpty()) {
      throw new IllegalStateException("authentication.token cannot be empty");
    }
    headers = Collections.singletonMap("Authorization", "Bearer " + token);
  }

  @Override
  public Map<String, String> getHTTPAuthenticationHeaders() {
    return headers;
  }

  @Override
  public void close() {
  }
}
