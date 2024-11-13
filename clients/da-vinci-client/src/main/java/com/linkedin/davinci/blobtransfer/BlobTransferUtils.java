package com.linkedin.davinci.blobtransfer;

import io.netty.handler.codec.http.HttpResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;


public class BlobTransferUtils {
  public static final String BLOB_TRANSFER_STATUS = "X-Blob-Transfer-Status";
  public static final String BLOB_TRANSFER_COMPLETED = "Completed";
  public static final String BLOB_TRANSFER_TYPE = "X-Blob-Transfer-Type";
  public static final long BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD = 1000L;

  public enum BlobTransferType {
    FILE, METADATA
  }

  /**
   * Check if the HttpResponse message is for metadata.
   * @param msg the HttpResponse message
   * @return true if the message is a metadata message, false otherwise
   */
  public static boolean isMetadataMessage(HttpResponse msg) {
    String metadataHeader = msg.headers().get(BlobTransferUtils.BLOB_TRANSFER_TYPE);
    if (metadataHeader == null) {
      return false;
    }
    return metadataHeader.equals(BlobTransferUtils.BlobTransferType.METADATA.name());
  }

  /**
   * Generate MD5 checksum for a file
   * @param filePath the path to the file
   * @return a hex string
   * @throws IOException if an I/O error occurs
   */
  public static String generateFileChecksum(Path filePath) throws IOException {
    String md5Hex;
    try (InputStream inputStream = Files.newInputStream(filePath)) {
      md5Hex = org.apache.commons.codec.digest.DigestUtils.md5Hex(inputStream);
    } catch (IOException e) {
      throw new IOException("Failed to generate checksum for file: " + filePath.toAbsolutePath(), e);
    }
    return md5Hex;
  }
}
