package com.linkedin.venice.hadoop.ssl;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.ConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;


public class TempFileSSLConfigurator implements SSLConfigurator {
  private static Logger logger = Logger.getLogger(TempFileSSLConfigurator.class);

  @Override
  public Properties setupSSLConfig(Properties props, Credentials userCredentials) {
    Properties properties = new Properties();
    properties.putAll(props);
    try {
      // Setup keystore certification
      byte[] keyStoreCert =
          getCertification(userCredentials, new Text(properties.getProperty(KafkaPushJob.SSL_KEY_STORE_PROPERTY_NAME)));
      logger.info("Found key store cert from credentials.");
      String keyStoreLocation = writeToTempFile(keyStoreCert);
      logger.info("Write key store cert to file: " + keyStoreLocation);
      properties.put(SSL_KEYSTORE_LOCATION, keyStoreLocation);
      // Setup truststore certification
      byte[] truestStoreCert =
          getCertification(userCredentials, new Text(properties.getProperty(KafkaPushJob.SSL_TRUST_STORE_PROPERTY_NAME)));
      logger.info("Found trust store cert from credentials.");
      String trustStoreLocation = writeToTempFile(truestStoreCert);
      logger.info("Write trust store cert to file: " + trustStoreLocation);
      properties.put(SSL_TRUSTSTORE_LOCATION, trustStoreLocation);
    } catch (VeniceException e) {
      if(properties.containsKey(KAFKA_SECURITY_PROTOCOL) && properties.getProperty(KAFKA_SECURITY_PROTOCOL).toLowerCase().equals("ssl")) {
        throw e;
      } else {
        logger.warn("Ignore the error because we are using non-ssl protocol.", e);
      }
    }

    return properties;
  }

  protected byte[] getCertification(Credentials credentials, Text certName) {
    byte[] cert = credentials.getSecretKey(certName);
    if (cert == null) {
      throw new VeniceException("Could not find certification: " + certName.toString() + " under user's credentials.");
    }
    return cert;
  }

  protected String writeToTempFile(byte[] certification) {
    File file;
    try {
      Path path = Files.createTempFile(null, null);
      IOUtils.copyBytes(new ByteArrayInputStream(certification), Files.newOutputStream(path),
          (long) certification.length, true);
      file = path.toFile();
      // Clean up the cert temp file once job completed.
      // If jvm is terminated unexpectedly, the file will be left, but as it's the tmp file so it should be collected
      // eventually.
      file.deleteOnExit();
      if (certification.length != file.length() || !file.setReadable(true, true)) {
        throw new VeniceException("Unable to create or chmod file: " + file.getAbsolutePath());
      }
      return file.getCanonicalPath();
    } catch (IOException e) {
      throw new VeniceException("Unable to create temp file for certification.", e);
    }
  }
}
