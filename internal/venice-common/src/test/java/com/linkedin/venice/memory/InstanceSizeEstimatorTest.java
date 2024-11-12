package com.linkedin.venice.memory;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;

import com.linkedin.davinci.kafka.consumer.StoreBufferService;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
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
    super(ImmutablePubSubMessage.class);
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
    empiricalInstanceMeasurement(Put.class, rtPutSupplier);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    /** The {@link PubSubTopicPartition} is supposed to be a shared instance, but it cannot be null. */
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("topic"), 0);

    VeniceWriter.DefaultLeaderMetadata defaultLeaderMetadata = new VeniceWriter.DefaultLeaderMetadata("blah");

    List<Supplier<KafkaMessageEnvelope>> kmeSuppliers = new ArrayList<>();
    kmeSuppliers.add(
        () -> new KafkaMessageEnvelope(
            // What a message in the RT topic might look like
            PUT.getValue(),
            producerMetadataSupplier.get(),
            rtPutSupplier.get(),
            // The (improperly-named) "leader" metadata footer is always populated, but in this path it points to a
            // static instance.
            defaultLeaderMetadata));
    kmeSuppliers.add(() -> {
      // What a VT message produced by a leader replica might look like
      byte[] rawKafkaValue = new byte[50];
      return new KafkaMessageEnvelope(
          PUT.getValue(),
          producerMetadataSupplier.get(),
          new Put(ByteBuffer.wrap(rawKafkaValue, 10, 10), 1, 1, ByteBuffer.wrap(rawKafkaValue, 25, 10)),
          new LeaderMetadata(
              null, // shared instance
              0L,
              0));
    });
    // TODO: Add updates, deletes...

    for (Supplier<KafkaMessageEnvelope> kmeSupplier: kmeSuppliers) {
      empiricalInstanceMeasurement(KafkaMessageEnvelope.class, kmeSupplier);
    }

    BiFunction<Supplier<KafkaKey>, Supplier<KafkaMessageEnvelope>, PubSubMessage> psmProvider =
        (kafkaKeySupplier, kmeSupplier) -> new ImmutablePubSubMessage<>(
            kafkaKeySupplier.get(),
            kmeSupplier.get(),
            pubSubTopicPartition,
            0,
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
            StoreBufferService.QueueNode.class,
            () -> new StoreBufferService.QueueNode(psmProvider.apply(kafkaKeySupplier, kmeSupplier), null, null, 0));
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
