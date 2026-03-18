package com.linkedin.venice.samza;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriterHook;
import org.apache.samza.config.MapConfig;
import org.testng.annotations.Test;


public class VeniceSystemProducerConfigTest {
  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "storeName cannot be null")
  public void testBuilderRejectsNullStoreName() {
    new VeniceSystemProducerConfig.Builder().setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setVerifyLatestProtocolPresent(true)
        .build();
  }

  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "samzaJobId cannot be null")
  public void testBuilderRejectsNullSamzaJobId() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setRunningFabric("dc-0")
        .setDiscoveryUrl("http://discovery")
        .build();
  }

  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "pushType cannot be null")
  public void testBuilderRejectsNullPushType() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setVerifyLatestProtocolPresent(true)
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*discoveryUrl.*ZK hosts.*")
  public void testBuilderRejectsDiscoveryUrlWithZkHosts() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setVerifyLatestProtocolPresent(true)
        .setDiscoveryUrl("http://discovery")
        .setVeniceChildD2ZkHost("zk:2181")
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*discoveryUrl.*D2 clients.*")
  public void testBuilderRejectsDiscoveryUrlWithD2Clients() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setDiscoveryUrl("http://discovery")
        .setProvidedChildColoD2Client(mock(D2Client.class))
        .setProvidedPrimaryControllerColoD2Client(mock(D2Client.class))
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*D2 clients.*ZK hosts.*")
  public void testBuilderRejectsD2ClientsWithZkHosts() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setProvidedChildColoD2Client(mock(D2Client.class))
        .setProvidedPrimaryControllerColoD2Client(mock(D2Client.class))
        .setVeniceChildD2ZkHost("zk:2181")
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Discovery URL must not be empty.*")
  public void testBuilderRejectsEmptyDiscoveryUrl() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setDiscoveryUrl("  ")
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*childColoD2Client.*primaryControllerColoD2Client.*must be set together")
  public void testBuilderRejectsPartialD2Clients() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setProvidedChildColoD2Client(mock(D2Client.class))
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*childColoD2Client.*primaryControllerColoD2Client.*must be set together")
  public void testMixedD2ClientsFailsAtBuildTime() {
    new VeniceSystemProducerConfig.Builder().setStoreName("testStore")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("testJobId")
        .setRunningFabric("testFabric")
        .setFactory(mock(VeniceSystemFactory.class))
        .setPrimaryControllerD2ServiceName("primaryServiceName")
        .setProvidedPrimaryControllerColoD2Client(mock(D2Client.class))
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*At least one connection mode.*")
  public void testBuilderRejectsNoConnectionMode() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*veniceChildD2ZkHost.*primaryControllerColoD2ZKHost.*must be set together")
  public void testBuilderRejectsPartialZkHosts() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setVeniceChildD2ZkHost("zk:2181")
        .setPrimaryControllerD2ServiceName("ChildController")
        .build();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*primaryControllerD2ServiceName.*required.*")
  public void testBuilderRejectsMissingD2ServiceNameForZkMode() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setVeniceChildD2ZkHost("zk:2181")
        .setPrimaryControllerColoD2ZKHost("zk:2181")
        .build();
  }

  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "time cannot be null")
  public void testBuilderRejectsNullTime() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setDiscoveryUrl("http://discovery")
        .setTime(null)
        .build();
  }

  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "factory cannot be null")
  public void testBuilderRejectsNullFactory() {
    new VeniceSystemProducerConfig.Builder().setStoreName("store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("job-id")
        .setRunningFabric("dc-0")
        .setDiscoveryUrl("http://discovery")
        .build();
  }

  @Test
  public void testToBuilderRoundTrip() {
    VeniceSystemFactory factory = mock(VeniceSystemFactory.class);
    SSLFactory sslFactory = mock(SSLFactory.class);
    Time time = mock(Time.class);
    VeniceWriterHook writerHook = mock(VeniceWriterHook.class);
    MapConfig samzaConfig = new MapConfig();

    VeniceSystemProducerConfig original = new VeniceSystemProducerConfig.Builder().setStoreName("testStore")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("testJobId")
        .setRunningFabric("testFabric")
        .setVerifyLatestProtocolPresent(true)
        .setDiscoveryUrl("http://discovery")
        .setFactory(factory)
        .setSslFactory(sslFactory)
        .setPartitioners("com.example.Partitioner")
        .setTime(time)
        .setWriterHook(writerHook)
        .setSamzaConfig(samzaConfig)
        .setRouterUrl("http://router")
        .build();

    VeniceSystemProducerConfig copy = original.toBuilder().build();

    assertNotSame(copy, original);
    assertEquals(copy.getStoreName(), original.getStoreName());
    assertEquals(copy.getPushType(), original.getPushType());
    assertEquals(copy.getSamzaJobId(), original.getSamzaJobId());
    assertEquals(copy.getRunningFabric(), original.getRunningFabric());
    assertEquals(copy.isVerifyLatestProtocolPresent(), original.isVerifyLatestProtocolPresent());
    assertEquals(copy.getDiscoveryUrl(), original.getDiscoveryUrl());
    assertSame(copy.getFactory(), original.getFactory());
    assertEquals(copy.getSslFactory(), original.getSslFactory());
    assertEquals(copy.getPartitioners(), original.getPartitioners());
    assertSame(copy.getTime(), original.getTime());
    assertSame(copy.getWriterHook(), original.getWriterHook());
    assertSame(copy.getSamzaConfig(), original.getSamzaConfig());
    assertEquals(copy.getRouterUrl(), original.getRouterUrl());
  }

  @Test
  public void testToBuilderOverrideWriterHook() {
    VeniceSystemFactory factory = mock(VeniceSystemFactory.class);
    SSLFactory sslFactory = mock(SSLFactory.class);
    Time time = mock(Time.class);
    VeniceWriterHook originalHook = mock(VeniceWriterHook.class);
    VeniceWriterHook newHook = mock(VeniceWriterHook.class);
    MapConfig samzaConfig = new MapConfig();

    VeniceSystemProducerConfig original = new VeniceSystemProducerConfig.Builder().setStoreName("testStore")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("testJobId")
        .setRunningFabric("testFabric")
        .setVerifyLatestProtocolPresent(true)
        .setDiscoveryUrl("http://discovery")
        .setFactory(factory)
        .setSslFactory(sslFactory)
        .setPartitioners("com.example.Partitioner")
        .setTime(time)
        .setWriterHook(originalHook)
        .setSamzaConfig(samzaConfig)
        .setRouterUrl("http://router")
        .build();

    VeniceSystemProducerConfig modified = original.toBuilder().setWriterHook(newHook).build();

    assertSame(modified.getWriterHook(), newHook);
    assertSame(original.getWriterHook(), originalHook);

    // All other fields remain the same
    assertEquals(modified.getStoreName(), original.getStoreName());
    assertEquals(modified.getPushType(), original.getPushType());
    assertEquals(modified.getSamzaJobId(), original.getSamzaJobId());
    assertEquals(modified.getRunningFabric(), original.getRunningFabric());
    assertEquals(modified.isVerifyLatestProtocolPresent(), original.isVerifyLatestProtocolPresent());
    assertEquals(modified.getDiscoveryUrl(), original.getDiscoveryUrl());
    assertSame(modified.getFactory(), original.getFactory());
    assertEquals(modified.getSslFactory(), original.getSslFactory());
    assertEquals(modified.getPartitioners(), original.getPartitioners());
    assertSame(modified.getTime(), original.getTime());
    assertSame(modified.getSamzaConfig(), original.getSamzaConfig());
    assertEquals(modified.getRouterUrl(), original.getRouterUrl());
  }
}
