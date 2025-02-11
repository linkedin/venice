package com.linkedin.venice.exceptions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;


public class VeniceUnauthorizedAccessExceptionTest {
  @Test
  public void testExceptionWithMessage() {
    String message = "Unauthorized access to the resource";
    VeniceUnauthorizedAccessException exception = new VeniceUnauthorizedAccessException(message);

    assertNotNull(exception);
    assertEquals(exception.getMessage(), message);
  }

  @Test
  public void testExceptionWithMessageAndCause() {
    String message = "Unauthorized access due to invalid credentials";
    Throwable cause = new RuntimeException("Invalid credentials");
    VeniceUnauthorizedAccessException exception = new VeniceUnauthorizedAccessException(message, cause);

    assertNotNull(exception);
    assertEquals(exception.getMessage(), message);
    assertEquals(exception.getCause(), cause);
  }
}
