package com.linkedin.alpini.io;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import javax.annotation.WillClose;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum IOUtils {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final Logger LOG = LogManager.getLogger(IOUtils.class);

  public static final int EOF = -1;

  public static void closeQuietly(@WillClose Closeable stream) {
    try (Closeable closeable = stream) {
      LOG.debug("closeQuietly({})", closeable);
    } catch (IOException ex) {
      LOG.debug("Error closing stream {}", stream, ex);
    }
  }

  public static long copy(InputStream inputStream, OutputStream outputStream) throws IOException {
    return copy(inputStream, outputStream, 8192);
  }

  public static long copy(InputStream inputStream, OutputStream outputStream, final int bufferSize) throws IOException {
    return copy(inputStream, outputStream, new byte[bufferSize]);
  }

  public static long copy(InputStream inputStream, OutputStream outputStream, byte[] buffer) throws IOException {
    long totalBytesCopied = 0;
    int bytesRead;
    while (EOF != (bytesRead = inputStream.read(buffer))) {
      outputStream.write(buffer, 0, bytesRead);
      totalBytesCopied += bytesRead;
    }
    return totalBytesCopied;
  }

  public static byte[] toByteArray(InputStream inputStream) throws IOException {
    return toByteArray(inputStream, new byte[4096]);
  }

  public static byte[] toByteArray(InputStream inputStream, byte[] buffer) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int bytesRead;
    while (EOF != (bytesRead = inputStream.read(buffer))) {
      baos.write(buffer, 0, bytesRead);
    }
    return baos.toByteArray();
  }

  public static String toString(InputStream inputStream, Charset charset) throws IOException {
    return toString(inputStream, charset, new char[4096]);
  }

  public static String toString(InputStream inputStream, Charset charset, char[] buffer) throws IOException {
    StringWriter writer = new StringWriter();
    InputStreamReader reader = new InputStreamReader(inputStream, charset);
    int charsRead;
    while (EOF != (charsRead = reader.read(buffer))) {
      writer.write(buffer, 0, charsRead);
    }
    return writer.toString();
  }
}
