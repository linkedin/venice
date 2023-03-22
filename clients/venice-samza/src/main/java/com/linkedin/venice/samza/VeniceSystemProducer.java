package com.linkedin.venice.samza;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.NearlineProducer;
import com.linkedin.venice.producer.ProducerMessageEnvelope;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.io.Closeable;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;


/**
 * {@code VeniceSystemProducer} defines the interfaces for Samza jobs to send data to Venice stores.
 *
 * Samza jobs talk to either parent or child controller depending on the aggregate mode config.
 * The decision of which controller should be used is made in {@link VeniceSystemFactory}.
 * The "Primary Controller" term is used to refer to whichever controller the Samza job should talk to.
 *
 * The primary controller should be:
 * 1. The parent controller when the Venice system is deployed in a multi-colo mode and either:
 *     a. {@link Version.PushType} is {@link PushType.BATCH} or {@link PushType.STREAM_REPROCESSING}; or
 *     b. @deprecated {@link Version.PushType} is {@link PushType.STREAM} and the job is configured to write data in AGGREGATE mode
 * 2. The child controller when either:
 *     a. The Venice system is deployed in a single-colo mode; or
 *     b. The {@link Version.PushType} is {@link PushType.STREAM} and the job is configured to write data in NON_AGGREGATE mode
 */
public class VeniceSystemProducer extends NearlineProducer implements SystemProducer, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemProducer.class);

  @Deprecated
  public VeniceSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    this(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}. Equivalent to {@link VeniceSystemProducer(veniceChildD2ZkHost, primaryControllerColoD2ZKHost, primaryControllerD2ServiceName, storeName, pushType, samzaJobId, runningFabric, verifyLatestProtocolPresent, factory, sslFactory, partitioners, SystemTime.INSTANCE)}
   */
  public VeniceSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    this(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners,
        SystemTime.INSTANCE);
  }

  @Deprecated
  public VeniceSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    this(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners,
        time);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link Version.PushType} to use to write to the store
   * @param samzaJobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningFabric The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link VeniceSystemFactory} object that was used to create this object
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   * @param time An object of type {@link Time}. It is helpful to be configurable for testing.
   */
  public VeniceSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    super(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners,
        time);
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  @Override
  public void register(String source) {

  }

  @Override
  public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
    ProducerMessageEnvelope producerMessageEnvelope = new ProducerMessageEnvelope(
        outgoingMessageEnvelope.getSystemStream().getStream(),
        outgoingMessageEnvelope.getKey(),
        outgoingMessageEnvelope.getMessage());
    send(source, producerMessageEnvelope);
  }

  /**
   * Flushing the data to Venice store in case VeniceSystemProducer buffers message.
   *
   * @param s String representing the source of the message. Currently, VeniceSystemProducer is not using this param.
   */
  @Override
  public void flush(String s) {
    super.flush();
  }
}
