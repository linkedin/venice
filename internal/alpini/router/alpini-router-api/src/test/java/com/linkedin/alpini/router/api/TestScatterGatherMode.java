package com.linkedin.alpini.router.api;

import static org.mockito.Mockito.any;

import com.linkedin.alpini.base.misc.CollectionUtil;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 6/20/17.
 */
public class TestScatterGatherMode {
  @DataProvider
  public Object[][] broadcastModes() {
    return new Object[][] { new Object[] { ScatterGatherMode.BROADCAST_BY_PARTITION },
        new Object[] { ScatterGatherMode.BROADCAST_BY_PRIMARY_HOST } };
  }

  @DataProvider
  public Object[][] scatterModes() {
    return new Object[][] { new Object[] { ScatterGatherMode.GROUP_BY_PARTITION },
        new Object[] { ScatterGatherMode.GROUP_BY_PRIMARY_HOST },
        new Object[] { ScatterGatherMode.GROUP_BY_GREEDY_HOST }, };
  }

  @Test(groups = "unit")
  public void testCollection() {
    HashSet<ScatterGatherMode> modes = new HashSet<>();
    modes.add(ScatterGatherMode.BROADCAST_BY_PARTITION);
    modes.add(ScatterGatherMode.BROADCAST_BY_PRIMARY_HOST);
    modes.add(ScatterGatherMode.GROUP_BY_PARTITION);
    modes.add(ScatterGatherMode.GROUP_BY_PRIMARY_HOST);
    modes.add(ScatterGatherMode.GROUP_BY_GREEDY_HOST);
    Assert.assertEquals(modes.size(), 5);
  }

  @Test(groups = "unit", dataProvider = "broadcastModes")
  public void testAsBroadcastMode(ScatterGatherMode mode) {
    Assert.assertSame(mode.asBroadcast(), mode);
    try {
      mode.asScatter();
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
  }

  @Test(groups = "unit", dataProvider = "scatterModes")
  public void testAsScatterMode(ScatterGatherMode mode) {
    Assert.assertSame(mode.asScatter(), mode);
    try {
      mode.asBroadcast();
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  <H, P extends ResourcePath<K>, K> Scatter<H, P, K> mockScatter() {
    return Mockito.mock(Scatter.class);
  }

  @SuppressWarnings("unchecked")
  <K> PartitionFinder<K> mockPartitionFinder() {
    return Mockito.mock(PartitionFinder.class);
  }

  @SuppressWarnings("unchecked")
  <H, R> HostFinder<H, R> mockHostFinder() {
    return Mockito.mock(HostFinder.class);
  }

  @SuppressWarnings("unchecked")
  <H> HostHealthMonitor<H> mockHostHealthMonitor() {
    return Mockito.mock(HostHealthMonitor.class);
  }

  @SuppressWarnings("unchecked")
  <H, K> ArgumentCaptor<ScatterGatherRequest<H, K>> scatterGatherRequestCaptor() {
    return (ArgumentCaptor) ArgumentCaptor.forClass(ScatterGatherRequest.class);
  }

  @Test(groups = "unit", dataProvider = "broadcastModes")
  public <P extends ResourcePath<K>, K> void testBasicBroadcastOfflinePartition(ScatterGatherMode mode)
      throws RouterException {
    Scatter<Host, P, K> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<K> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(Collections.singletonList("OnePartition"))
        .when(partitionFinder)
        .getAllPartitionNames(Mockito.eq(resourceName));

    Mockito.doReturn(Collections.singletonList(host))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(false).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host), Mockito.eq("OnePartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ArgumentCaptor<ScatterGatherRequest<Host, K>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOfflineRequest(scatterGatherCaptor.capture());

    Mockito.verifyNoMoreInteractions(scatter);

    ScatterGatherRequest<Host, K> scatterGather = scatterGatherCaptor.getValue();
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("OnePartition"));
    Assert.assertEquals(scatterGather.getHosts(), Collections.emptyList());
  }

  @Test(groups = "unit", dataProvider = "broadcastModes")
  public <P extends ResourcePath<K>, K> void testBasicBroadcastOnePartition(ScatterGatherMode mode)
      throws RouterException {
    Scatter<Host, P, K> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<K> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(Collections.singletonList("OnePartition"))
        .when(partitionFinder)
        .getAllPartitionNames(Mockito.eq(resourceName));

    Mockito.doReturn(Collections.singletonList(host))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host), Mockito.eq("OnePartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ArgumentCaptor<ScatterGatherRequest<Host, K>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verifyNoMoreInteractions(scatter);

    ScatterGatherRequest<Host, K> scatterGather = scatterGatherCaptor.getValue();
    Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host));
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("OnePartition"));
  }

  @Test(groups = "unit", dataProvider = "broadcastModes")
  public <P extends ResourcePath<K>, K> void testBasicBroadcastTwoPartitions(ScatterGatherMode mode)
      throws RouterException {
    Scatter<Host, P, K> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<K> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class);
    Host host2 = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(CollectionUtil.listOf("OnePartition", "TwoPartition"))
        .when(partitionFinder)
        .getAllPartitionNames(Mockito.eq(resourceName));

    Mockito.doReturn(Collections.singletonList(host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(Collections.singletonList(host2))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host1), Mockito.eq("OnePartition"));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host2), Mockito.eq("TwoPartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ScatterGatherRequest<Host, K> scatterGather;
    ArgumentCaptor<ScatterGatherRequest<Host, K>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter, Mockito.times(2)).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verifyNoMoreInteractions(scatter);

    Iterator<ScatterGatherRequest<Host, K>> scatterGatherIterator = scatterGatherCaptor.getAllValues().iterator();

    scatterGather = scatterGatherIterator.next();
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("OnePartition"));
    Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host1));
    scatterGather = scatterGatherIterator.next();
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("TwoPartition"));
    Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host2));
    Assert.assertFalse(scatterGatherIterator.hasNext());
  }

  @Test(groups = "unit", dataProvider = "broadcastModes")
  public <P extends ResourcePath<K>, K> void testBasicBroadcastOnlineOfflinePartitions(ScatterGatherMode mode)
      throws RouterException {
    Scatter<Host, P, K> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<K> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class);
    Host host2 = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(CollectionUtil.listOf("OnePartition", "TwoPartition"))
        .when(partitionFinder)
        .getAllPartitionNames(Mockito.eq(resourceName));

    Mockito.doReturn(Collections.singletonList(host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(Collections.singletonList(host2))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host1), Mockito.eq("OnePartition"));

    Mockito.doReturn(false).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host2), Mockito.eq("TwoPartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ScatterGatherRequest<Host, K> scatterGather;
    ArgumentCaptor<ScatterGatherRequest<Host, K>> scatterGatherOnlineCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOnlineRequest(scatterGatherOnlineCaptor.capture());
    ArgumentCaptor<ScatterGatherRequest<Host, K>> scatterGatherOfflineCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOfflineRequest(scatterGatherOfflineCaptor.capture());

    Mockito.verifyNoMoreInteractions(scatter);

    Iterator<ScatterGatherRequest<Host, K>> scatterGatherIterator = scatterGatherOnlineCaptor.getAllValues().iterator();

    scatterGather = scatterGatherIterator.next();
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("OnePartition"));
    Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host1));
    Assert.assertFalse(scatterGatherIterator.hasNext());

    scatterGatherIterator = scatterGatherOfflineCaptor.getAllValues().iterator();
    scatterGather = scatterGatherIterator.next();
    Assert.assertEquals(scatterGather.getPartitionNamesToQuery(), Collections.singletonList("TwoPartition"));
    Assert.assertEquals(scatterGather.getHosts(), Collections.emptyList());
    Assert.assertFalse(scatterGatherIterator.hasNext());
  }

  @Test(groups = "unit", dataProvider = "scatterModes")
  void testBasicScatterOfflinePartition(ScatterGatherMode mode) throws RouterException {
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);
    Key key = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(Collections.singletonList(key)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition").when(partitionFinder).findPartitionName(Mockito.eq(resourceName), Mockito.eq(key));

    Mockito.doReturn(Collections.singletonList(host))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(false).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host), Mockito.eq("OnePartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ArgumentCaptor<ScatterGatherRequest<Host, Key>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOfflineRequest(scatterGatherCaptor.capture());

    Mockito.verify(scatter).getPath();
    Mockito.verifyNoMoreInteractions(scatter);

    ScatterGatherRequest<Host, Key> scatterGather = scatterGatherCaptor.getValue();
    Assert.assertEquals(scatterGather.getPartitionKeys(), Collections.singletonList(key));
    Assert.assertEquals(scatterGather.getHosts(), Collections.emptyList());
  }

  @Test(groups = "unit", dataProvider = "scatterModes")
  void testBasicScatterOnePartition(ScatterGatherMode mode) throws RouterException {
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host = Mockito.mock(Host.class);
    Role roles = Mockito.mock(Role.class);
    Key key = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(Collections.singletonList(key)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition").when(partitionFinder).findPartitionName(Mockito.eq(resourceName), Mockito.eq(key));

    Mockito.doReturn(Collections.singletonList(host))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host), Mockito.eq("OnePartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    Mockito.verify(hostHealthMonitor).isHostHealthy(host, "OnePartition");
    Mockito.verifyNoMoreInteractions(hostHealthMonitor);

    ArgumentCaptor<ScatterGatherRequest<Host, Key>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verify(scatter).getPath();
    Mockito.verifyNoMoreInteractions(scatter);

    ScatterGatherRequest<Host, Key> scatterGather = scatterGatherCaptor.getValue();
    Assert.assertEquals(scatterGather.getPartitionKeys(), Collections.singletonList(key));
    Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host));
  }

  @Test(groups = "unit", dataProvider = "scatterModes")
  void testBasicScatterTwoPartitions(ScatterGatherMode mode) throws RouterException {
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class);
    Host host2 = Mockito.mock(Host.class);
    Key key1 = Mockito.mock(Key.class);
    Key key2 = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(CollectionUtil.listOf(key1, key2)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key1));

    Mockito.doReturn("TwoPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key2));

    Mockito.doReturn(Collections.singletonList(host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(Collections.singletonList(host2))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host1), Mockito.eq("OnePartition"));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host2), Mockito.eq("TwoPartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    Mockito.verify(hostHealthMonitor).isHostHealthy(host1, "OnePartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host2, "TwoPartition");
    Mockito.verifyNoMoreInteractions(hostHealthMonitor);

    ScatterGatherRequest<Host, Key> scatterGather;
    ArgumentCaptor<ScatterGatherRequest<Host, Key>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter, Mockito.times(2)).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verify(scatter).getPath();
    Mockito.verifyNoMoreInteractions(scatter);

    Iterator<ScatterGatherRequest<Host, Key>> scatterGatherIterator =
        scatterGatherCaptor.getAllValues().stream().iterator();

    while (scatterGatherIterator.hasNext()) {
      scatterGather = scatterGatherIterator.next();
      if (scatterGather.getPartitionKeys().contains(key1)) {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host1));
      } else {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host2));
      }
    }
    Assert.assertFalse(scatterGatherIterator.hasNext());
  }

  @Test(groups = "unit")
  void testGroupByHost() throws RouterException {
    ScatterGatherMode mode = ScatterGatherMode.GROUP_BY_PRIMARY_HOST;
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class);
    Host host2 = Mockito.mock(Host.class);
    Key key1 = Mockito.mock(Key.class);
    Key key2 = Mockito.mock(Key.class);
    Key key3 = Mockito.mock(Key.class);
    Key key4 = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(CollectionUtil.listOf(key1, key2, key3, key4)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key1));

    Mockito.doReturn("TwoPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key2));

    Mockito.doReturn("ThreePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key3));

    Mockito.doReturn("FourPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key4));

    Mockito.doReturn(CollectionUtil.listOf(host1, host2))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host1, host2))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("ThreePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("FourPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host1), Mockito.eq("OnePartition"));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host2), Mockito.eq("TwoPartition"));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host1), Mockito.eq("ThreePartition"));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(Mockito.eq(host2), Mockito.eq("FourPartition"));

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    Mockito.verify(hostHealthMonitor).isHostHealthy(host1, "OnePartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host2, "OnePartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host1, "TwoPartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host2, "TwoPartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host1, "ThreePartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host2, "ThreePartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host1, "FourPartition");
    Mockito.verify(hostHealthMonitor).isHostHealthy(host2, "FourPartition");
    Mockito.verifyNoMoreInteractions(hostHealthMonitor);

    ScatterGatherRequest<Host, Key> scatterGather;
    ArgumentCaptor<ScatterGatherRequest<Host, Key>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter, Mockito.times(2)).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verify(scatter).getPath();
    Mockito.verifyNoMoreInteractions(scatter);

    Iterator<ScatterGatherRequest<Host, Key>> scatterGatherIterator =
        scatterGatherCaptor.getAllValues().stream().iterator();

    while (scatterGatherIterator.hasNext()) {
      scatterGather = scatterGatherIterator.next();
      if (scatterGather.getPartitionKeys().contains(key1)) {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host1));
        Assert.assertEqualsNoOrder(
            scatterGather.getPartitionKeys().stream().toArray(Key[]::new),
            new Key[] { key1, key3 });
      } else {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host2));
        Assert.assertEqualsNoOrder(
            scatterGather.getPartitionKeys().stream().toArray(Key[]::new),
            new Key[] { key2, key4 });
      }
    }
    Assert.assertFalse(scatterGatherIterator.hasNext());
  }

  @Test(groups = "unit")
  void testGroupByGreedyHost() throws RouterException {
    ScatterGatherMode mode = ScatterGatherMode.GROUP_BY_GREEDY_HOST;
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class, "host1");
    Host host2 = Mockito.mock(Host.class, "host2");
    Host host3 = Mockito.mock(Host.class, "host3");
    Key key1 = Mockito.mock(Key.class);
    Key key2 = Mockito.mock(Key.class);
    Key key3 = Mockito.mock(Key.class);
    Key key4 = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(CollectionUtil.listOf(key1, key2, key3, key4)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key1));

    Mockito.doReturn("TwoPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key2));

    Mockito.doReturn("ThreePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key3));

    Mockito.doReturn("FourPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key4));

    Mockito.doReturn(CollectionUtil.listOf(host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host3))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host1, host2, host3))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("ThreePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host3))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("FourPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(any(Host.class), Mockito.anyString());

    Assert.assertSame(
        CompletableFuture
            .completedFuture(
                mode.scatter(
                    scatter,
                    requestMethod,
                    resourceName,
                    AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                    hostFinder,
                    hostHealthMonitor,
                    roles))
            .thenCompose(Function.identity())
            .join(),
        scatter);

    ScatterGatherRequest<Host, Key> scatterGather;
    ArgumentCaptor<ScatterGatherRequest<Host, Key>> scatterGatherCaptor = scatterGatherRequestCaptor();
    Mockito.verify(scatter, Mockito.times(2)).addOnlineRequest(scatterGatherCaptor.capture());

    Mockito.verify(scatter).getPath();
    Mockito.verifyNoMoreInteractions(scatter);

    Iterator<ScatterGatherRequest<Host, Key>> scatterGatherIterator =
        scatterGatherCaptor.getAllValues().stream().iterator();

    while (scatterGatherIterator.hasNext()) {
      scatterGather = scatterGatherIterator.next();
      if (scatterGather.getPartitionKeys().contains(key1)) {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host1));
        Assert.assertEquals(scatterGather.getPartitionKeys(), Collections.singletonList(key1));
      } else {
        Assert.assertEquals(scatterGather.getHosts(), Collections.singletonList(host3));
        Assert.assertEqualsNoOrder(
            scatterGather.getPartitionKeys().stream().toArray(Key[]::new),
            new Key[] { key2, key3, key4 });
      }
    }
    Assert.assertFalse(scatterGatherIterator.hasNext());
  }

  @Test(groups = "unit", expectedExceptions = RouterException.class)
  void testGroupByException() throws Throwable {
    ScatterGatherMode mode = ScatterGatherMode.GROUP_BY_PRIMARY_HOST;
    Scatter<Host, Path, Key> scatter = mockScatter();
    String requestMethod = "GET";
    String resourceName = "/Hello/World";
    PartitionFinder<Key> partitionFinder = mockPartitionFinder();
    HostFinder<Host, Role> hostFinder = mockHostFinder();
    HostHealthMonitor<Host> hostHealthMonitor = mockHostHealthMonitor();
    Host host1 = Mockito.mock(Host.class, "host1");
    Host host2 = Mockito.mock(Host.class, "host2");
    Host host3 = Mockito.mock(Host.class, "host3");
    Key key1 = Mockito.mock(Key.class);
    Key key2 = Mockito.mock(Key.class);
    Key key3 = Mockito.mock(Key.class);
    Key key4 = Mockito.mock(Key.class);
    Path path = Mockito.mock(Path.class);
    Role roles = Mockito.mock(Role.class);

    Mockito.doReturn(path).when(scatter).getPath();
    Mockito.doReturn(CollectionUtil.listOf(key1, key2, key3, key4)).when(path).getPartitionKeys();

    Mockito.doReturn("OnePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key1));

    Mockito.doReturn("TwoPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key2));

    Mockito.doReturn("ThreePartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key3));

    Mockito.doReturn("FourPartition")
        .when(partitionFinder)
        .findPartitionName(Mockito.eq(resourceName), Mockito.eq(key4));

    Mockito.doReturn(CollectionUtil.listOf(host1))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("OnePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host3))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("TwoPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doThrow(RouterException.class)
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("ThreePartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(CollectionUtil.listOf(host2, host3))
        .when(hostFinder)
        .findHosts(
            Mockito.eq(requestMethod),
            Mockito.eq(resourceName),
            Mockito.eq("FourPartition"),
            any(),
            Mockito.eq(roles));

    Mockito.doReturn(true).when(hostHealthMonitor).isHostHealthy(any(Host.class), Mockito.anyString());

    try {
      CompletableFuture
          .completedFuture(
              mode.scatter(
                  scatter,
                  requestMethod,
                  resourceName,
                  AsyncPartitionFinder.adapt(partitionFinder, Runnable::run),
                  hostFinder,
                  hostHealthMonitor,
                  roles))
          .thenCompose(Function.identity())
          .join();
      Assert.fail();
    } catch (CompletionException ex) {
      throw ex.getCause();
    } finally {
      Mockito.verify(scatter).getPath();
      Mockito.verifyNoMoreInteractions(scatter);
    }
  }

  private interface Key extends Comparable<Key> {
    @Override
    default int compareTo(Key other) {
      return String.CASE_INSENSITIVE_ORDER.compare(toString(), other.toString());
    }
  }

  private interface Path extends ResourcePath<Key> {
  }

  private interface Host {
  }

  private interface Role {
  }
}
