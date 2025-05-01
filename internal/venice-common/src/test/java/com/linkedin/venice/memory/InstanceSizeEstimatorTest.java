package com.linkedin.venice.memory;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;

import com.linkedin.davinci.kafka.consumer.LeaderProducedRecordContext;
import com.linkedin.davinci.kafka.consumer.SBSQueueNodeFactory;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.testng.annotations.Test;


public class InstanceSizeEstimatorTest extends HeapSizeEstimatorTest {
  public InstanceSizeEstimatorTest() {
    super(LeaderProducedRecordContext.class);
  }

  @Test
  public void testInstanceMeasurement() {
    printHeader(TestMethodology.EMPIRICAL_MEASUREMENT.resultsTableHeader);

    // Try various array sizes to take alignment into account.
    List<Supplier<KafkaKey>> kafkaKeySuppliers = new ArrayList<>();
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[0]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[2]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[4]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[6]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[8]));
    kafkaKeySuppliers.add(() -> new KafkaKey(PUT, new byte[10]));

    for (Supplier kafkaKeySupplier: kafkaKeySuppliers) {
      empiricalInstanceMeasurement(KafkaKey.class, kafkaKeySupplier);
    }

    Supplier<ProducerMetadata> producerMetadataSupplier = () -> new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    empiricalInstanceMeasurement(ProducerMetadata.class, producerMetadataSupplier);

    Supplier<Put> rtPutSupplier = () -> new Put(ByteBuffer.allocate(10), 1, -1, null);
    Supplier<Put> vtPutSupplier = () -> {
      byte[] rawKafkaValue = new byte[50];
      return new Put(ByteBuffer.wrap(rawKafkaValue, 10, 10), 1, 1, ByteBuffer.wrap(rawKafkaValue, 25, 10));
    };
    List<Supplier<Put>> putSuppliers = new ArrayList<>();
    putSuppliers.add(rtPutSupplier);
    putSuppliers.add(vtPutSupplier);
    for (Supplier<Put> putSupplier: putSuppliers) {
      empiricalInstanceMeasurement(Put.class, putSupplier);
    }

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    /** The {@link PubSubTopicPartition} is supposed to be a shared instance, but it cannot be null. */
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("topic"), 0);

    VeniceWriter.DefaultLeaderMetadata defaultLeaderMetadata = new VeniceWriter.DefaultLeaderMetadata("blah");

    List<Supplier<KafkaMessageEnvelope>> kmeSuppliers = new ArrayList<>();
    Supplier<KafkaMessageEnvelope> rtKmeSupplier = () -> new KafkaMessageEnvelope(
        // What a message in the RT topic might look like
        PUT.getValue(),
        producerMetadataSupplier.get(),
        rtPutSupplier.get(),
        // The (improperly-named) "leader" metadata footer is always populated, but in this path it points to a
        // static instance.
        defaultLeaderMetadata);
    Supplier<KafkaMessageEnvelope> vtKmeSupplier = () -> new KafkaMessageEnvelope(
        PUT.getValue(),
        producerMetadataSupplier.get(),
        vtPutSupplier.get(),
        new LeaderMetadata(
            null, // shared instance
            0L,
            0));
    kmeSuppliers.add(rtKmeSupplier);
    kmeSuppliers.add(vtKmeSupplier);
    // TODO: Add updates, deletes...

    for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
      empiricalInstanceMeasurement(KafkaMessageEnvelope.class, kmeSupplier);
    }

    BiFunction<Supplier<KafkaKey>, Supplier<KafkaMessageEnvelope>, DefaultPubSubMessage> psmProvider =
        (kafkaKeySupplier, kmeSupplier) -> new ImmutablePubSubMessage(
            kafkaKeySupplier.get(),
            kmeSupplier.get(),
            pubSubTopicPartition,
            ApacheKafkaOffsetPosition.of(0),
            0,
            0);

    for (Supplier<KafkaKey> kafkaKeySupplier: kafkaKeySuppliers) {
      for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
        empiricalInstanceMeasurement(
            ImmutablePubSubMessage.class,
            () -> psmProvider.apply(kafkaKeySupplier, kmeSupplier));
      }
    }

    for (Supplier<KafkaKey> kafkaKeySupplier: kafkaKeySuppliers) {
      for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
        empiricalInstanceMeasurement(
            SBSQueueNodeFactory.queueNodeClass(),
            () -> SBSQueueNodeFactory.queueNode(psmProvider.apply(kafkaKeySupplier, kmeSupplier), null, null, 0));
      }
    }

    int kafkaClusterId = 0;
    Supplier<LeaderProducedRecordContext> leaderProducedRecordContextSupplierForPut =
        () -> LeaderProducedRecordContext.newPutRecord(kafkaClusterId, 0, new byte[10], vtPutSupplier.get());
    List<Supplier<LeaderProducedRecordContext>> leaderProducedRecordContextSuppliers = new ArrayList<>();
    leaderProducedRecordContextSuppliers.add(leaderProducedRecordContextSupplierForPut);

    for (Supplier<LeaderProducedRecordContext> leaderProducedRecordContextSupplier: leaderProducedRecordContextSuppliers) {
      empiricalInstanceMeasurement(LeaderProducedRecordContext.class, leaderProducedRecordContextSupplier);
    }

    for (Supplier<KafkaKey> kafkaKeySupplier: kafkaKeySuppliers) {
      for (Supplier<LeaderProducedRecordContext> leaderProducedRecordContextSupplier: leaderProducedRecordContextSuppliers) {
        empiricalInstanceMeasurement(
            SBSQueueNodeFactory.leaderQueueNodeClass(),
            () -> SBSQueueNodeFactory.leaderQueueNode(
                psmProvider.apply(kafkaKeySupplier, vtKmeSupplier),
                null,
                null,
                0,
                leaderProducedRecordContextSupplier.get()));
      }
    }

    printResultSeparatorLine();
  }

  private void empiricalInstanceMeasurement(Class c, Supplier<?> constructor) {
    Object o = constructor.get();
    int expectedSize = InstanceSizeEstimator.getObjectSize(o);
    empiricalMeasurement(c, expectedSize, constructor);
  }
}
