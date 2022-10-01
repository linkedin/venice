package com.linkedin.venice;

/**
 * Common config keys shared by venice-backend and venice-thin-client.
 */
public class CommonConfigKeys {
  // ssl config TODO: update the query.sh script inside venice-thin-client MP to use the new config
  public static final String SSL_ENABLED = "ssl.enabled";
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
  public static final String SSL_KEY_PASSWORD = "ssl.key.password";
  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
  public static final String SSL_KEYMANAGER_ALGORITHM = "ssl.keymanager.algorithm";
  public static final String SSL_TRUSTMANAGER_ALGORITHM = "ssl.trustmanager.algorithm";
  public static final String SSL_SECURE_RANDOM_IMPLEMENTATION = "ssl.secure.random.implementation";

  /**
   * This config mainly control the SSL behavior inside AdminSparkServer.
   *
   * With SSL_NEEDS_CLIENT_CERT set to false, the AdminSparkServer will not check the certificate sent by client, which
   * should be the default behavior in venice OSS project because AdminSparkServer doesn't accept the self signed
   * certificate created in test cases.
   *
   * In production, we need to override this config to true to check client certificate.
   */
  public static final String SSL_NEEDS_CLIENT_CERT = "ssl.needs.client.cert";

  /**
   * This config defines the class name of the SSL factory.
   *
   * a. In the livenice, backend and VPJ product, we should override this config with the
   *    LinkedIn internal SSL factory class name.
   * b. In open source venice project, we should use the {@link com.linkedin.venice.security.DefaultSSLFactory}
   *    class, which is a clone implementation of {@link com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl}
   *    (Container 25.2.15)
   */
  public static final String SSL_FACTORY_CLASS_NAME = "ssl.factory.class.name";
}
