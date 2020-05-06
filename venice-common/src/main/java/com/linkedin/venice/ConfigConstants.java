package com.linkedin.venice;

import com.linkedin.venice.utils.Time;


public class ConfigConstants {
  /**
   * Start of controller config default value
   */

  /**
   * Default value of sleep interval for polling topic deletion status from ZK.
   */
  public static final int DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS = 2 * Time.MS_PER_SECOND;

  /**
   * End of controller config default value
   */

  // Start of server config default value

  /**
   * Default Kafka SSL context provider class name.
   *
   * {@link org.apache.kafka.common.security.ssl.BoringSslContextProvider} supports openssl.
   * BoringSSL is the c implementation of OpenSSL, and conscrypt add a java wrapper around BoringSSL.
   * The default BoringSslContextProvider mainly relies on conscrypt.
   */
  public static final String DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME = "org.apache.kafka.common.security.ssl.BoringSslContextProvider";
  // End of server config default value
}
