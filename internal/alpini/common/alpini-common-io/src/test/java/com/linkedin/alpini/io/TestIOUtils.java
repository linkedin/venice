package com.linkedin.alpini.io;

import com.linkedin.alpini.base.misc.Time;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/15/17.
 */
public class TestIOUtils {

  // private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(IOUtils.class);
  @Test(groups = "unit")
  public void testCloseQuietly() throws IOException {
    // org.apache.log4j.Appender appender = Mockito.mock(org.apache.log4j.Appender.class);
    try {
      // LOG.addAppender(appender);

      Throwable ex = new IOException();

      Closeable stream = Mockito.mock(Closeable.class);
      Mockito.doThrow(ex).when(stream).close();

      IOUtils.closeQuietly(stream);

      Mockito.verify(stream).close();

      // ArgumentCaptor<org.apache.log4j.spi.LoggingEvent> eventArgumentCaptor =
      // ArgumentCaptor.forClass(org.apache.log4j.spi.LoggingEvent.class);
      // Mockito.verify(appender).doAppend(eventArgumentCaptor.capture());

      // org.apache.log4j.spi.LoggingEvent event = eventArgumentCaptor.getValue();
      // Assert.assertSame(event.getThrowableInformation().getThrowable(), ex);

      Mockito.verifyNoMoreInteractions(stream);
      // Mockito.verifyNoMoreInteractions(stream, appender);
    } finally {
      // LOG.removeAppender(appender);
    }
  }

  @Test(groups = "unit")
  public void testToByteArray() throws IOException {
    Random rnd = new Random(Time.nanoTime());
    byte[] source = new byte[10000];
    rnd.nextBytes(source);
    byte[] bytes = IOUtils.toByteArray(new ByteArrayInputStream(source));
    Assert.assertNotSame(bytes, source);
    Assert.assertEquals(bytes, source);
  }

  @Test(groups = "unit")
  public void testToString() throws IOException {
    StringBuilder sb = new StringBuilder(11000);
    while (sb.length() < 10000) {
      sb.append("The quick brown fox jumps over the lazy dog");
    }
    String source = sb.toString();
    String output = IOUtils
        .toString(new ByteArrayInputStream(source.getBytes(StandardCharsets.US_ASCII)), StandardCharsets.US_ASCII);
    Assert.assertNotSame(output, source);
    Assert.assertEquals(output, source);
  }
}
