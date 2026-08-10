package com.linkedin.venice.hadoop.ssl;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class UserCredentialsFactory {
  private static final Logger LOGGER = LogManager.getLogger(UserCredentialsFactory.class);
  public static final int REQUIRED_SECRET_KEY_COUNT = 4;

  /**
   * Get user's credentials from the current Hadoop user group context.
   */
  public static Credentials getHadoopUserCredentials() throws IOException {

    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    verifyCredentials(credentials);
    return credentials;
  }

  /**
   * Get user's credentials from the the Hadoop token file
   */
  public static Credentials getUserCredentialsFromTokenFile() throws IOException {
    String tokenFilePath = getTokenFilePath(
        System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION),
        System.getProperty(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION));
    File tokenFile = new File(tokenFilePath);
    Credentials credentials = Credentials.readTokenStorageFile(tokenFile, new Configuration());
    verifyCredentials(credentials);
    return credentials;
  }

  static String getTokenFilePath(String environmentValue, String systemPropertyValue) {
    String tokenFilePath = environmentValue != null ? environmentValue : systemPropertyValue;
    if (tokenFilePath == null) {
      throw new VeniceException(
          "Hadoop token file location is not configured. Set " + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION
              + " as an environment variable or system property.");
    }
    return tokenFilePath;
  }

  private static void verifyCredentials(Credentials credentials) {
    if (credentials.numberOfSecretKeys() < REQUIRED_SECRET_KEY_COUNT) {
      LOGGER.info("Number of tokens found: {}", credentials.numberOfTokens()); // Currently is 4
      LOGGER.warn("Number of secret keys found: {}", credentials.numberOfSecretKeys()); // Currently should be 4
      LOGGER.warn(
          "The current credentials does not contain required secret keys which are required by enabling Kafka SSL.");
    }
  }
}
