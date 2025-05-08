package com.linkedin.venice.beam.consumer;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceCoordinateOutOfRangeException;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * Beam Connector for Venice Change data capture IO. Uses {@link VeniceChangelogConsumer} underneath
 * to pull messages from configured venice store.
 */
public final class VeniceChangelogConsumerIO {
  private static final Logger LOG = LogManager.getLogger(VeniceChangelogConsumerIO.class);

  private VeniceChangelogConsumerIO() {
  }

  public static <K, V> Read<K, V> read() {
    return new Read<>();
  }

  public static class Read<K, V>
      extends PTransform<PBegin, PCollection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>>> {
    private static final long serialVersionUID = 1L;

    public enum SeekWhence
        implements BiFunction<VeniceChangelogConsumer, CheckPointProperties, CompletableFuture<Void>> {
      CHECKPOINT {
        @Override
        public CompletableFuture<Void> apply(
            VeniceChangelogConsumer consumer,
            CheckPointProperties checkPointProperties) {
          Set<VeniceChangeCoordinate> coordinates = checkPointProperties.getCoordinates();
          LOG.info("Seeking To Coordinates {} for store {}", coordinates, checkPointProperties.getStore());
          return consumer.seekToCheckpoint(coordinates);
        }
      },
      END_OF_PUSH {
        @Override
        public CompletableFuture<Void> apply(
            VeniceChangelogConsumer consumer,
            CheckPointProperties checkPointProperties) {
          LOG.info("Seeking To EndOfPush for store {}", checkPointProperties.getStore());
          return consumer.seekToEndOfPush();
        }
      },
      START_OF_PUSH {
        @Override
        public CompletableFuture<Void> apply(
            VeniceChangelogConsumer consumer,
            CheckPointProperties checkPointProperties) {
          LOG.info("Seeking To BeginningOfPush for store {}", checkPointProperties.getStore());
          return consumer.seekToBeginningOfPush();
        }
      },
      TAIL {
        @Override
        public CompletableFuture<Void> apply(
            VeniceChangelogConsumer consumer,
            CheckPointProperties checkPointProperties) {
          LOG.info("Seeking To Tail for store {}", checkPointProperties.getStore());
          return consumer.seekToTail();
        }
      },
      TIMESTAMP {
        // Seeks to given epoch timestamp in milliseconds if positive or rewinds by given
        // milliseconds from current time if negative.
        @Override
        public CompletableFuture<Void> apply(
            VeniceChangelogConsumer consumer,
            CheckPointProperties checkPointProperties) {
          long seekTimestamp;
          if (checkPointProperties.getSeekTimestamp() < 0) {
            seekTimestamp = System.currentTimeMillis() + checkPointProperties.getSeekTimestamp();
          } else {
            seekTimestamp = checkPointProperties.getSeekTimestamp();
          }
          LOG.info("Seeking To Timestamp {} for store {}", seekTimestamp, checkPointProperties.getStore());
          return consumer.seekToTimestamp(seekTimestamp);
        }
      };
    }

    private Set<Integer> partitions = Collections.emptySet();
    private Duration pollTimeout = Duration.standardSeconds(1);
    // Positive timestamp is treated as epoch time in milliseconds, negative timestamp is treated as
    // rewind time in milliseconds (from current time)
    private long seekTimestamp;
    private SeekWhence seekWhence = SeekWhence.CHECKPOINT;

    private String store = "";
    private String consumerIdSuffix = "";
    private Duration terminationTimeout = Duration.standardSeconds(30);

    private LocalVeniceChangelogConsumerProvider localVeniceChangelogConsumerProvider;

    public Read() {
    }

    public Read(Read read) {
      this.consumerIdSuffix = read.consumerIdSuffix;
      this.partitions = read.partitions;
      this.pollTimeout = read.pollTimeout;
      this.seekWhence = read.seekWhence;
      this.store = read.store;
      this.terminationTimeout = read.terminationTimeout;
      this.seekTimestamp = read.seekTimestamp;
      this.localVeniceChangelogConsumerProvider = read.localVeniceChangelogConsumerProvider;
    }

    @Override
    public PCollection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> expand(PBegin input) {
      Source<K, V> source = new Source<>(this);
      Unbounded<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> unbounded =
          org.apache.beam.sdk.io.Read.from(source);
      return input.getPipeline().apply(unbounded);
    }

    public Duration getPollTimeout() {
      return this.pollTimeout;
    }

    public Read<K, V> setPollTimeout(Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    public SeekWhence getSeekWhence() {
      return this.seekWhence;
    }

    public Read<K, V> setSeekWhence(SeekWhence seekWhence) {
      this.seekWhence = seekWhence;
      return this;
    }

    public String getStore() {
      return this.store;
    }

    public Read<K, V> setStore(String store) {
      this.store = store;
      return this;
    }

    public Duration getTerminationTimeout() {
      return this.terminationTimeout;
    }

    public Read<K, V> setTerminationTimeout(Duration timeout) {
      this.terminationTimeout = timeout;
      return this;
    }

    public long getSeekTimestamp() {
      return seekTimestamp;
    }

    public Read<K, V> setSeekTimestamp(long seekTimestamp) {
      this.seekTimestamp = seekTimestamp;
      return this;
    }

    public Set<Integer> getPartitions() {
      return partitions;
    }

    public Read<K, V> setPartitions(Set<Integer> partitions) {
      this.partitions = partitions;
      return this;
    }

    public LocalVeniceChangelogConsumerProvider getLocalVeniceChangelogConsumerProvider() {
      return localVeniceChangelogConsumerProvider;
    }

    public Read<K, V> setLocalVeniceChangelogConsumerProvider(
        LocalVeniceChangelogConsumerProvider localVeniceChangelogConsumerProvider) {
      this.localVeniceChangelogConsumerProvider = localVeniceChangelogConsumerProvider;
      return this;
    }

    public String getConsumerIdSuffix() {
      return consumerIdSuffix;
    }

    public Read<K, V> setConsumerIdSuffix(String consumerIdSuffix) {
      this.consumerIdSuffix = consumerIdSuffix;
      return this;
    }

    public RemoveMetadata withoutMetadata() {
      return new RemoveMetadata();
    }

    public CurrentValueTransform withOnlyCurrentValue(Coder returnTypeCoder) {
      return new CurrentValueTransform(returnTypeCoder);
    }

    class RemoveMetadata extends PTransform<PBegin, PCollection<KV<K, ChangeEvent<V>>>> {
      private static final long serialVersionUID = 1L;

      @Override
      public @Nonnull PCollection<KV<K, ChangeEvent<V>>> expand(PBegin pBegin) {
        PCollection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> input = pBegin.apply(Read.this);
        return input.apply(
            MapElements.via(
                new SimpleFunction<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>, KV<K, ChangeEvent<V>>>() {
                  @Override
                  public KV<K, ChangeEvent<V>> apply(PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message) {
                    return KV.of(message.getKey(), message.getValue());
                  }
                }))
            .setCoder(VeniceMessageCoder.of());
      }
    }

    private class CurrentValueTransform extends PTransform<PBegin, PCollection<KV<K, GenericRecord>>> {
      private static final long serialVersionUID = 1L;
      private final Coder<KV<K, GenericRecord>> _returnTypeCoder;

      CurrentValueTransform(Coder<KV<K, GenericRecord>> returnTypeCoder) {
        _returnTypeCoder = Objects.requireNonNull(returnTypeCoder);
      }

      @Override
      public @Nonnull PCollection<KV<K, GenericRecord>> expand(PBegin pBegin) {
        PCollection<KV<K, ChangeEvent<V>>> pCollection = pBegin.apply(new RemoveMetadata());
        return pCollection.apply(MapElements.via(new SimpleFunction<KV<K, ChangeEvent<V>>, KV<K, GenericRecord>>() {
          @Override
          public KV<K, GenericRecord> apply(KV<K, ChangeEvent<V>> message) {
            GenericData.Record value = (GenericData.Record) message.getValue().getCurrentValue();
            return KV.of(message.getKey(), value);
          }
        })).setCoder(_returnTypeCoder);
      }
    }
  }

  private static class Source<K, V>
      extends UnboundedSource<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>, VeniceCheckpointMark> {
    private static final long serialVersionUID = 1L;
    private final Read<K, V> read;

    Source(Read<K, V> read) {
      this.read = read;
    }

    @Override
    public @Nonnull List<? extends Source<K, V>> split(int desiredNumSplits, @Nonnull PipelineOptions options)
        throws Exception {
      Set<Integer> partitions = this.read.getPartitions();
      if (partitions.isEmpty()) {
        partitions =
            IntStream
                .range(
                    0,
                    this.read.localVeniceChangelogConsumerProvider.getVeniceChangelogConsumer(this.read.getStore())
                        .getPartitionCount())
                .boxed()
                .collect(Collectors.toSet());
        LOG.info("Detected store {} has {} partitions", this.read.getStore(), partitions.size());
      }

      // NOTE: Enforces all splits have the same # of partitions.
      int numSplits = Math.min(desiredNumSplits, partitions.size());
      while (partitions.size() % numSplits > 0) {
        ++numSplits;
      }

      List<Set<Integer>> partitionSplits = new ArrayList<>(numSplits);
      for (int i = 0; i < numSplits; ++i) {
        partitionSplits.add(new HashSet<>());
      }
      for (int partition: partitions) {
        partitionSplits.get(partition % numSplits).add(partition);
      }

      List<Source<K, V>> sourceSplits = new ArrayList<>(numSplits);
      for (Set<Integer> partitionSplit: partitionSplits) {
        sourceSplits.add(new Source<>(new Read<K, V>(this.read).setPartitions(partitionSplit)));
      }
      return sourceSplits;
    }

    @Override
    public @Nonnull UnboundedReader<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> createReader(
        @Nonnull PipelineOptions options,
        VeniceCheckpointMark checkpointMark) {
      LOG.debug("Creating reader for store {} and partitions {}", this.read.getStore(), this.read.getPartitions());
      return new PubSubMessageReader<>(this.read, checkpointMark);
    }

    @Override
    public @Nonnull Coder<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> getOutputCoder() {
      return PubSubMessageCoder.of();
    }

    @Override
    public @Nonnull Coder<VeniceCheckpointMark> getCheckpointMarkCoder() {
      /**
       * TODO: Figure out how to receive {@link PubSubPositionDeserializer) via {@link com.linkedin.davinci.consumer.ChangelogClientConfig}
       */
      return new VeniceCheckpointMark.Coder(PubSubPositionDeserializer.DEFAULT_DESERIALIZER);
    }
  }

  private static class PubSubMessageReader<K, V>
      extends UnboundedSource.UnboundedReader<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> {
    private final Read<K, V> read;
    private final Map<Integer, VeniceChangeCoordinate> _partitionToVeniceChangeCoordinates = new HashMap<>();

    // NOTE: Poll consumer using separate thread for performance.
    private final ExecutorService service = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>>> queue =
        new SynchronousQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private PeekingIterator<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> batch =
        Iterators.peekingIterator(Collections.emptyIterator());

    private final Gauge checkpointPubsubTimestamp;
    private final Gauge checkpointPubsubLag;
    private final Counter revisedCheckpoints;
    private final Distribution sampledPayloadSize;
    private final Counter consumerPollCount;
    private final Counter queuePollCount;

    private VeniceChangelogConsumer<K, V> consumer;

    PubSubMessageReader(Read<K, V> read, @Nullable VeniceCheckpointMark veniceCheckpointMark) {
      this.read = read;
      if (veniceCheckpointMark != null) {
        veniceCheckpointMark.getVeniceChangeCoordinates()
            .forEach(c -> _partitionToVeniceChangeCoordinates.put(c.getPartition(), c));
      }
      final String metricPrefix = String.join("_", this.read.store, this.read.partitions.toString());
      this.checkpointPubsubTimestamp =
          Metrics.gauge(VeniceChangelogConsumerIO.class, metricPrefix + "_CheckpointPubsubTimestamp");
      this.checkpointPubsubLag = Metrics.gauge(VeniceChangelogConsumerIO.class, metricPrefix + "_CheckpointPubsubLag");
      this.revisedCheckpoints = Metrics.counter(VeniceChangelogConsumerIO.class, metricPrefix + "_RevisedCheckpoints");
      this.sampledPayloadSize =
          Metrics.distribution(VeniceChangelogConsumerIO.class, metricPrefix + "_SampledPayloadSize");
      this.consumerPollCount = Metrics.counter(VeniceChangelogConsumerIO.class, metricPrefix + "_ConsumerPollCount");
      this.queuePollCount = Metrics.counter(VeniceChangelogConsumerIO.class, metricPrefix + "_QueuePollCount");
    }

    /**
     * Initializes veniceChangeLogConsumer and subscribes to partitions as prescribed by {@link
     * Source#split(int, PipelineOptions)}.
     */
    @SuppressWarnings("FutureReturnValueIgnored")
    @Override
    public boolean start() {
      try {
        this.consumer = this.read.localVeniceChangelogConsumerProvider.getVeniceChangelogConsumer(
            this.read.getStore(),
            this.read.getPartitions().toString() + this.read.consumerIdSuffix);
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }

      try {
        this.consumer.subscribe(this.read.partitions).get();
      } catch (ExecutionException | InterruptedException e) {
        throw new IllegalStateException(e);
      }

      Set<VeniceChangeCoordinate> veniceChangeCoordinates = read.partitions.stream()
          .map(partition -> _partitionToVeniceChangeCoordinates.get(partition))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      try {
        CheckPointProperties checkPointProperties =
            new CheckPointProperties(veniceChangeCoordinates, read.seekTimestamp, read.store);
        this.read.getSeekWhence().apply(this.consumer, checkPointProperties).get();
      } catch (ExecutionException | InterruptedException e) {
        LOG.error(
            "Store={} failed to {}={} for partitions={}",
            this.read.store,
            this.read.seekWhence,
            veniceChangeCoordinates,
            this.read.partitions,
            e);
        if (!(e.getCause() instanceof VeniceCoordinateOutOfRangeException)) {
          throw new IllegalStateException(e);
        }
        LOG.warn("SeekingToEndOfPush because checkpoint is likely beyond retention.");
        try {
          this.consumer.seekToEndOfPush().get();
        } catch (ExecutionException | InterruptedException ee) {
          throw new IllegalStateException(ee);
        }
      }

      // Keep on pulling messages in background
      this.service.submit(this::consumerPollLoop);
      return advance();
    }

    /** Adds messages to the queue if venice change capture consumer returns any on polling. */
    private void consumerPollLoop() {
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> messages = Collections.emptyList();
      do {
        if (messages.isEmpty()) {
          messages = this.consumer.poll(this.read.getPollTimeout().getMillis());
          consumerPollCount.inc();
          LOG.debug(
              "Polled & received {} messages from the consumer for partitions {}",
              messages.size(),
              this.read.getPartitions());
          continue;
        }

        try {
          // NOTE(SynchronousQueue): Block and wait for another thread to receive.
          this.queue.put(messages);
          LOG.debug("Added {} messages to queue for partitions {}", messages.size(), this.read.getPartitions());
          messages = Collections.emptyList();
        } catch (InterruptedException e) {
          LOG.error("{} consumer thread interrupted", this, e);
          break; // exit
        }
      } while (!this.closed.get());
      LOG.info("{}: Returning from consumer pool loop", this);
    }

    /** Fetches latest checkpoint and updates _partitionToVeniceChangeCoordinates. */
    void reviseCheckpoints() {
      PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> pubSubMessage = getCurrent();
      if (pubSubMessage == null) {
        return;
      }

      VeniceChangeCoordinate veniceChangeCoordinate = pubSubMessage.getOffset();
      LOG.debug("Revised checkpoint for partition {}", veniceChangeCoordinate.getPartition());
      _partitionToVeniceChangeCoordinates.put(veniceChangeCoordinate.getPartition(), veniceChangeCoordinate);

      this.checkpointPubsubTimestamp.set(pubSubMessage.getPubSubMessageTime());
      this.checkpointPubsubLag.set(System.currentTimeMillis() - pubSubMessage.getPubSubMessageTime());
      this.revisedCheckpoints.inc();
      this.sampledPayloadSize.update(pubSubMessage.getPayloadSize());
    }

    /**
     * Polls messages from the reader and makes them available in queue for consumption. Also
     * revises checkpoints whenever the iterator advances.
     */
    @Override
    public boolean advance() {
      if (this.batch.hasNext()) {
        this.batch.next();
        // Return if messages are present or else if exhausted continue polling for new messages
        if (this.batch.hasNext()) {
          return true;
        }
      }

      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> messages;
      try {
        messages = this.queue.poll(this.read.getPollTimeout().getMillis(), TimeUnit.MILLISECONDS);
        queuePollCount.inc();
      } catch (InterruptedException e) {
        LOG.error("{} advancing thread interrupted", this, e);
        return false;
      }

      if (messages == null) {
        LOG.info(
            "{} advancing timed out for store {} and partitions {}",
            this,
            this.read.getStore(),
            this.read.getPartitions());
        return false;
      }

      this.batch = Iterators.peekingIterator(messages.iterator());
      reviseCheckpoints();
      LOG.debug(
          "Received messages for store {}, partitions {}, number of messages {}",
          this.read.getStore(),
          this.read.getPartitions(),
          messages.size());
      return true;
    }

    /**
     * Throws {@link NoSuchElementException} if called without iterator initialization else returns
     * next element.
     */
    @Override
    @Nullable
    public PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> getCurrent() {
      if (!this.batch.hasNext()) {
        throw new NoSuchElementException();
      }
      return this.batch.peek();
    }

    @Override
    public Instant getCurrentTimestamp() {
      PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> pubSubMessage = null;
      try {
        pubSubMessage = this.getCurrent();
      } catch (NoSuchElementException e) {
        LOG.debug("No element found defaulting to epoch time for watermark");
      }
      if (pubSubMessage == null) {
        return Instant.EPOCH;
      }
      return Instant.ofEpochMilli(this.getCurrent().getPubSubMessageTime());
    }

    @Override
    public void close() {
      LOG.info("Closing Venice IO connector for store {}, partitions {}", read.store, read.partitions);
      this.closed.set(true);
      this.service.shutdown();

      boolean isShutdown;
      do {
        this.queue.clear();
        try {
          isShutdown =
              this.service.awaitTermination(this.read.getTerminationTimeout().getStandardSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.warn("{} interrupted while waiting on consumer thread to terminate", this, e);
          throw new IllegalStateException(e);
        }
      } while (!isShutdown);

      // NOTE: Try-catch inside UnboundedSourceSystem closes all readers which otherwise causes NPEs
      // on instances not yet started.
      if (this.consumer == null) {
        return;
      }

      try {
        this.consumer.close();
      } catch (IllegalStateException e) {
        LOG.info(
            "Note: Consumer is shared across partitions. Failed to close consumer for store {}, partitions {} since it"
                + " might already be closed. Exception reason {}",
            read.store,
            read.partitions,
            e.getMessage());
      }
    }

    @Override
    public UnboundedSource<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>, VeniceCheckpointMark> getCurrentSource() {
      return new Source<>(this.read);
    }

    @Override
    public Instant getWatermark() {
      return this.getCurrentTimestamp();
    }

    @Override
    public VeniceCheckpointMark getCheckpointMark() {
      return new VeniceCheckpointMark(new ArrayList<>(this._partitionToVeniceChangeCoordinates.values()));
    }
  }
}
