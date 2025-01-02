package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/**
 * The composite writer contains a main writer and multiple child writers. The main writer will only perform the write
 * once all of its child writers are complete.
 * TODO The child writers are view writers. Ideally to avoid code duplication we should be using an array of
 * VeniceViewWriter here. However, the current implementation of VeniceViewWriter involves PCS which is something
 * specific to the ingestion path that we don't want to leak into venice-common.
 */
public class CompositeVeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private final VeniceWriter<K, V, U> mainWriter;
  private final VeniceWriter<K, V, U>[] childWriters;
  private final PubSubProducerCallback childCallback;

  private CompletableFuture<PubSubProduceResult> lastWriteFuture = CompletableFuture.completedFuture(null);

  public CompositeVeniceWriter(
      String topicName,
      VeniceWriter<K, V, U> mainWriter,
      VeniceWriter<K, V, U>[] childWriters,
      PubSubProducerCallback childCallback) {
    super(topicName);
    this.mainWriter = mainWriter;
    this.childWriters = childWriters;
    this.childCallback = childCallback;
  }

  @Override
  public void close(boolean gracefulClose) throws IOException {
    for (VeniceWriter veniceWriter: childWriters) {
      veniceWriter.close(gracefulClose);
    }
    mainWriter.close(gracefulClose);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return put(key, value, valueSchemaId, callback, null, false);
  }

  @Override
  public Future<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return put(key, value, valueSchemaId, callback, putMetadata, true);
  }

  CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata,
      boolean hasPutMetadata) {
    CompletableFuture<PubSubProduceResult> finalFuture = new CompletableFuture<>();
    CompletableFuture[] childFutures = new CompletableFuture[childWriters.length + 1];
    int index = 0;
    childFutures[index++] = lastWriteFuture;
    for (VeniceWriter<K, V, U> writer: childWriters) {
      childFutures[index++] = hasPutMetadata
          ? writer.put(
              key,
              value,
              valueSchemaId,
              childCallback,
              DEFAULT_LEADER_METADATA_WRAPPER,
              APP_DEFAULT_LOGICAL_TS,
              putMetadata)
          : writer.put(key, value, valueSchemaId, childCallback);
    }
    CompletableFuture.allOf(childFutures).whenCompleteAsync((ignored, childException) -> {
      if (childException == null) {
        CompletableFuture<PubSubProduceResult> mainFuture = hasPutMetadata
            ? mainWriter.put(
                key,
                value,
                valueSchemaId,
                callback,
                DEFAULT_LEADER_METADATA_WRAPPER,
                APP_DEFAULT_LOGICAL_TS,
                putMetadata)
            : mainWriter.put(key, value, valueSchemaId, callback);
        mainFuture.whenCompleteAsync((result, mainWriteException) -> {
          if (mainWriteException == null) {
            finalFuture.complete(result);
          } else {
            finalFuture.completeExceptionally(new VeniceException(mainWriteException));
          }
        });
      } else {
        VeniceException veniceException = new VeniceException(childException);
        finalFuture.completeExceptionally(veniceException);
      }
    });
    lastWriteFuture = finalFuture;
    return finalFuture;
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    CompletableFuture<PubSubProduceResult> finalFuture = new CompletableFuture<>();
    CompletableFuture[] childFutures = new CompletableFuture[childWriters.length + 1];
    int index = 0;
    childFutures[index++] = lastWriteFuture;
    for (VeniceWriter<K, V, U> writer: childWriters) {
      childFutures[index++] = writer.delete(key, callback, deleteMetadata);
    }
    CompletableFuture.allOf(childFutures).whenCompleteAsync((ignored, childException) -> {
      if (childException == null) {
        CompletableFuture<PubSubProduceResult> mainFuture = mainWriter.delete(key, callback, deleteMetadata);
        mainFuture.whenCompleteAsync((result, mainWriteException) -> {
          if (mainWriteException == null) {
            finalFuture.complete(result);
          } else {
            finalFuture.completeExceptionally(new VeniceException(mainWriteException));
          }
        });
      } else {
        VeniceException veniceException = new VeniceException(childException);
        finalFuture.completeExceptionally(veniceException);
      }
    });

    lastWriteFuture = finalFuture;
    return finalFuture;
  }

  /**
   * The main use of the {@link CompositeVeniceWriter} for now is to write batch portion of a store version to VT and
   * materialized view topic in the NR fabric. Updates should never go through the {@link CompositeVeniceWriter} because
   * it should be written to RT (hybrid writes or incremental push) and handled by view writers in L/F or A/A SIT.
   */
  @Override
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName() + "does not support update function");
  }

  @Override
  public void flush() {
    for (VeniceWriter writer: childWriters) {
      writer.flush();
    }
    mainWriter.flush();
    try {
      // wait for queued writes to be executed
      lastWriteFuture.get();
    } catch (Exception e) {
      throw new VeniceException("Exception caught while waiting for queued writes to complete", e);
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }
}
