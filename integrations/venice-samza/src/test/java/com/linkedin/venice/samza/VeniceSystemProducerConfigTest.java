package com.linkedin.venice.samza;

import static org.mockito.Mockito.mock;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.meta.Version;
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
}
