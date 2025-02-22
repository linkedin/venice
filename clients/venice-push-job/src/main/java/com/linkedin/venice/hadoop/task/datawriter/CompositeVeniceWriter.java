package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;


/**
 * The composite writer contains a main writer and multiple child writers. The main writer will only perform the write
 * once all of its child writers are complete.
 * TODO The child writers are view writers. Ideally to avoid code duplication we should be using an array of
 * VeniceViewWriter here. However, the current implementation of VeniceViewWriter involves PCS which is something
 * specific to the ingestion path that we don't want to leak into venice-common.
 */
@NotThreadsafe
public class CompositeVeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private final VeniceWriter<K, V, U> mainWriter;
  private final AbstractVeniceWriter<K, V, U>[] childWriters;
  private final PubSubProducerCallback childCallback;

  public CompositeVeniceWriter(
      String topicName,
      VeniceWriter<K, V, U> mainWriter,
      AbstractVeniceWriter<K, V, U>[] childWriters,
      PubSubProducerCallback childCallback) {
    super(topicName);
    this.mainWriter = mainWriter;
    this.childWriters = childWriters;
    this.childCallback = childCallback;
  }

  @Override
  public void close(boolean gracefulClose) throws IOException {
    // no op, child writers and the main writer are initialized outside the class and should be closed elsewhere.
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return compositeOperation(
        (writer, writeCallback) -> writer.put(key, value, valueSchemaId, writeCallback),
        childCallback,
        callback);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return compositeOperation(
        (writer, writeCallback) -> writer.put(key, value, valueSchemaId, writeCallback, putMetadata),
        childCallback,
        callback);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    return compositeOperation(
        (writer, writeCallback) -> writer.delete(key, writeCallback, deleteMetadata),
        childCallback,
        callback);
  }

  /**
   * The main use of the {@link com.linkedin.venice.writer.CompositeVeniceWriter} for now is to write batch portion of a store version to VT and
   * materialized view topic in the NR fabric. Updates should never go through the {@link com.linkedin.venice.writer.CompositeVeniceWriter} because
   * it should be written to RT (hybrid writes or incremental push) and handled by view writers in L/F or A/A SIT.
   */
  @Override
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support update function");
  }

  @Override
  public void flush() {
    for (AbstractVeniceWriter writer: childWriters) {
      writer.flush();
    }
    mainWriter.flush();
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  /**
   * Helper function to perform a composite operation where the childWriterOp is first executed for all childWriters
   * and then mainWriterOp is executed for mainWriter. The returned completable future is completed when the mainWriter
   * completes the mainWriterOp.
   */
  private CompletableFuture<PubSubProduceResult> compositeOperation(
      BiFunction<AbstractVeniceWriter<K, V, U>, PubSubProducerCallback, CompletableFuture<PubSubProduceResult>> writerOperation,
      PubSubProducerCallback childWriterCallback,
      PubSubProducerCallback mainWriterCallback) {
    CompletableFuture<PubSubProduceResult> finalFuture = new CompletableFuture<>();
    CompletableFuture<PubSubProduceResult>[] writeFutures = new CompletableFuture[childWriters.length + 1];
    int index = 0;
    writeFutures[index++] = writerOperation.apply(mainWriter, mainWriterCallback);
    for (AbstractVeniceWriter<K, V, U> writer: childWriters) {
      writeFutures[index++] = writerOperation.apply(writer, childWriterCallback);
    }
    CompletableFuture.allOf(writeFutures).whenCompleteAsync((ignored, writeException) -> {
      if (writeException == null) {
        try {
          finalFuture.complete(writeFutures[0].get());
        } catch (Exception e) {
          // This shouldn't be possible since we already checked for exception earlier
          finalFuture.completeExceptionally(
              new IllegalStateException("CompletableFuture get() throwing exception unexpectedly"));
        }
      } else {
        finalFuture.completeExceptionally(new VeniceException(writeException));
      }
    });
    return finalFuture;
  }
}
