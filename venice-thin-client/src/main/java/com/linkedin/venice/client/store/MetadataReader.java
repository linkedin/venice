package com.linkedin.venice.client.store;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class MetadataReader implements Closeable {
  private final static Logger logger = LogManager.getLogger(MetadataReader.class);

  protected final AbstractAvroStoreClient storeClient;

  protected MetadataReader(AbstractAvroStoreClient storeClient) {
    this.storeClient = storeClient;
  }

  protected byte[] storeClientGetRawWithRetry(String requestPath) throws ExecutionException, InterruptedException {
    int attempt = 0;
    boolean retry = true;
    byte[] response = null;
    while (retry) {
      retry = false;
      try {
        CompletableFuture<byte[]> future = (CompletableFuture<byte[]>) storeClient.getRaw(requestPath);
        response = future.get();
      } catch (ExecutionException ee) {
        if (attempt++ > 3) {
          throw ee;
        } else {
          retry = true;
          logger.warn("Failed to get metadata from path: " + requestPath + " retrying...", ee);
        }
      }
    }
    return response;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(storeClient, logger::error);
  }
}
