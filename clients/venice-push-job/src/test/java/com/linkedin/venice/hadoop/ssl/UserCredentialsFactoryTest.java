package com.linkedin.venice.hadoop.ssl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.annotations.Test;


public class UserCredentialsFactoryTest {
  @Test
  public void testTokenFilePathPrefersEnvironmentValue() {
    assertEquals(
        UserCredentialsFactory.getTokenFilePath("/environment/token-file", "/system-property/token-file"),
        "/environment/token-file");
  }

  @Test
  public void testTokenFilePathFallsBackToSystemPropertyValue() {
    assertEquals(
        UserCredentialsFactory.getTokenFilePath(null, "/system-property/token-file"),
        "/system-property/token-file");
  }

  @Test
  public void testTokenFilePathFailsWhenLocationIsMissing() {
    VeniceException exception =
        expectThrows(VeniceException.class, () -> UserCredentialsFactory.getTokenFilePath(null, null));

    assertEquals(
        exception.getMessage(),
        "Hadoop token file location is not configured. Set " + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION
            + " as an environment variable or system property.");
  }

  @Test
  public void testGetUserCredentialsFromTokenFileFailsForMalformedFile() throws Exception {
    File tokenFile = Files.createTempFile("user-credentials-factory-invalid", ".tokens").toFile();
    String tokenFileProperty = UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
    String previousTokenFile = System.getProperty(tokenFileProperty);
    try {
      Files.write(tokenFile.toPath(), "not-a-token-file".getBytes(StandardCharsets.UTF_8));
      System.setProperty(tokenFileProperty, tokenFile.getAbsolutePath());

      expectThrows(IOException.class, UserCredentialsFactory::getUserCredentialsFromTokenFile);
    } finally {
      if (previousTokenFile == null) {
        System.clearProperty(tokenFileProperty);
      } else {
        System.setProperty(tokenFileProperty, previousTokenFile);
      }
      Files.deleteIfExists(tokenFile.toPath());
    }
  }
}
