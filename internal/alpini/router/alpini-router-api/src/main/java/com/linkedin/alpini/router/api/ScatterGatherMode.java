package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 6/20/17.
 */
public abstract class ScatterGatherMode {
  private static final Logger LOG = LogManager.getLogger(ScatterGatherMode.class);

  private final String _name;
  private final boolean _broadcast;

  protected ScatterGatherMode(@Nonnull String name, boolean broadcast) {
    _name = Objects.requireNonNull(name, "name");
    _broadcast = broadcast;
  }

  @Nonnull
  public final ScatterGatherMode asBroadcast() {
    if (_broadcast) {
      return this;
    } else {
      throw new IllegalArgumentException("Not a broadcast mode: " + this);
    }
  }

  @Nonnull
  public final ScatterGatherMode asScatter() {
    if (!_broadcast) {
      return this;
    } else {
      throw new IllegalArgumentException("Not a scatter mode: " + this);
    }
  }

  /**
   * Deprecated method is compatible to old method signature.
   */
  @Deprecated
  @Nonnull
  public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull PartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles,
      Metrics metrics) throws RouterException {
    throw new AbstractMethodError();
  }

  @Nonnull
  public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull AsyncPartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles,
      Metrics metrics,
      String initialHost) {
    // default method simply ignores the initialHost.
    return scatter(
        scatter,
        requestMethod,
        resourceName,
        partitionFinder,
        hostFinder,
        hostHealthMonitor,
        roles,
        metrics);
  }

  @Nonnull
  public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull AsyncPartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles,
      Metrics metrics) {
    try {
      return CompletableFuture.completedFuture(scatter(scatter, requestMethod, resourceName, new PartitionFinder<K>() {
        @Nonnull
        @Override
        public String findPartitionName(@Nonnull String resourceName, @Nonnull K partitionKey) throws RouterException {
          return join(
              CompletableFuture.completedFuture(Pair.make(resourceName, partitionKey))
                  .thenCompose(partitionFinder::findPartitionName));
        }

        @Nonnull
        @Override
        public List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException {
          return join(
              CompletableFuture.completedFuture(resourceName).thenCompose(partitionFinder::getAllPartitionNames));
        }

        @Override
        public int getNumPartitions(@Nonnull String resourceName) throws RouterException {
          return join(CompletableFuture.completedFuture(resourceName).thenCompose(partitionFinder::getNumPartitions));
        }

        @Override
        public int findPartitionNumber(@Nonnull K partitionKey, int numPartitions, String storeName, int versionNumber)
            throws RouterException {
          return join(partitionFinder.findPartitionNumber(partitionKey, numPartitions, storeName, versionNumber));
        }
      }, hostFinder, hostHealthMonitor, roles, metrics));
    } catch (RouterException ex) {
      return failedFuture(ex);
    }
  }

  private static <T> CompletionStage<T> failedFuture(Throwable ex) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.obtrudeException(ex);
    return future;
  }

  private static <T> T join(CompletionStage<T> future) throws RouterException {
    try {
      return CompletableFuture.completedFuture(future).thenCompose(Function.identity()).join();
    } catch (CompletionException ex) {
      if (ex.getCause() instanceof RouterException) {
        throw (RouterException) ex.getCause();
      }
      throw ex;
    }
  }

  private static <K> AsyncPartitionFinder<K> sync(PartitionFinder<K> partitionFinder) {
    return new AsyncPartitionFinder<K>() {
      @Nonnull
      @Override
      public CompletionStage<String> findPartitionName(@Nonnull String resourceName, @Nonnull K partitionKey) {
        return CompletableFuture.supplyAsync(() -> {
          try {
            return partitionFinder.findPartitionName(resourceName, partitionKey);
          } catch (RouterException e) {
            throw new CompletionException(e);
          }
        }, Runnable::run);
      }

      @Nonnull
      @Override
      public CompletionStage<List<String>> getAllPartitionNames(@Nonnull String resourceName) {
        return CompletableFuture.supplyAsync(() -> {
          try {
            return partitionFinder.getAllPartitionNames(resourceName);
          } catch (RouterException e) {
            throw new CompletionException(e);
          }
        }, Runnable::run);
      }

      @Override
      public CompletionStage<Integer> getNumPartitions(@Nonnull String resourceName) {
        return CompletableFuture.supplyAsync(() -> {
          try {
            return partitionFinder.getNumPartitions(resourceName);
          } catch (RouterException e) {
            throw new CompletionException(e);
          }
        }, Runnable::run);
      }

      @Override
      public CompletionStage<Integer> findPartitionNumber(
          K partitionKey,
          int numPartitions,
          String storeName,
          int versionNumber) {
        return CompletableFuture.supplyAsync(() -> {
          try {
            return partitionFinder.findPartitionNumber(partitionKey, numPartitions, storeName, versionNumber);
          } catch (RouterException e) {
            throw new CompletionException(e);
          }
        }, Runnable::run);
      }
    };
  }

  @Override
  public String toString() {
    return _name;
  }

  @Override
  public int hashCode() {
    return _name.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScatterGatherMode that = (ScatterGatherMode) o;
    return _broadcast == that._broadcast && _name.equals(that._name);
  }

  /**
   * Sends request to every partition.
   */
  public static final ScatterGatherMode BROADCAST_BY_PARTITION = new ScatterGatherMode("BROADCAST_BY_PARTITION", true) {
    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics metrics) throws RouterException {
      return join(
          scatter(
              scatter,
              requestMethod,
              resourceName,
              sync(partitionFinder),
              hostFinder,
              hostHealthMonitor,
              roles,
              metrics));
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          null);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          initialHost);
    }

    private <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatterBase(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {
      return partitionFinder.getAllPartitionNames(resourceName).thenApply(partitions -> {
        try {
          for (String partitionName: partitions) {
            HostHealthChecked<H> healthChecked = new HostHealthChecked<>(hostHealthMonitor);
            List<H> hosts =
                hostFinder.findHosts(requestMethod, resourceName, partitionName, healthChecked, roles, initialHost);
            BroadcastScatterGatherRequest<H, K> request;
            if ((hosts = healthChecked.check(hosts, partitionName)).isEmpty()) { // SUPPRESS CHECKSTYLE InnerAssignment
              request = new BroadcastScatterGatherRequest<>(Collections.emptyList());
              scatter.addOfflineRequest(request);
            } else {
              request = new BroadcastScatterGatherRequest<>(hosts);
              scatter.addOnlineRequest(request);
            }
            request.addPartitionNameToQuery(partitionName);
          }
          return scatter;
        } catch (RouterException e) {
          throw new CompletionException(e);
        }
      });
    }
  };

  /**
   * Sends request to the first host found for every partition, grouped by host.
   */
  public static final ScatterGatherMode BROADCAST_BY_PRIMARY_HOST =
      new ScatterGatherMode("BROADCAST_BY_PRIMARY_HOST", true) {
        @Nonnull
        @Override
        public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
            @Nonnull Scatter<H, P, K> scatter,
            @Nonnull String requestMethod,
            @Nonnull String resourceName,
            @Nonnull PartitionFinder<K> partitionFinder,
            @Nonnull HostFinder<H, R> hostFinder,
            @Nonnull HostHealthMonitor<H> hostHealthMonitor,
            @Nonnull R roles,
            Metrics metrics) throws RouterException {
          return join(
              scatter(
                  scatter,
                  requestMethod,
                  resourceName,
                  sync(partitionFinder),
                  hostFinder,
                  hostHealthMonitor,
                  roles,
                  metrics));
        }

        @Nonnull
        @Override
        public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
            @Nonnull Scatter<H, P, K> scatter,
            @Nonnull String requestMethod,
            @Nonnull String resourceName,
            @Nonnull AsyncPartitionFinder<K> partitionFinder,
            @Nonnull HostFinder<H, R> hostFinder,
            @Nonnull HostHealthMonitor<H> hostHealthMonitor,
            @Nonnull R roles,
            Metrics m) {
          return scatterBase(
              scatter,
              requestMethod,
              resourceName,
              partitionFinder,
              hostFinder,
              hostHealthMonitor,
              roles,
              m,
              null);
        }

        @Nonnull
        @Override
        public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
            @Nonnull Scatter<H, P, K> scatter,
            @Nonnull String requestMethod,
            @Nonnull String resourceName,
            @Nonnull AsyncPartitionFinder<K> partitionFinder,
            @Nonnull HostFinder<H, R> hostFinder,
            @Nonnull HostHealthMonitor<H> hostHealthMonitor,
            @Nonnull R roles,
            Metrics m,
            String initialHost) {
          return scatterBase(
              scatter,
              requestMethod,
              resourceName,
              partitionFinder,
              hostFinder,
              hostHealthMonitor,
              roles,
              m,
              initialHost);
        }

        private <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatterBase(
            @Nonnull Scatter<H, P, K> scatter,
            @Nonnull String requestMethod,
            @Nonnull String resourceName,
            @Nonnull AsyncPartitionFinder<K> partitionFinder,
            @Nonnull HostFinder<H, R> hostFinder,
            @Nonnull HostHealthMonitor<H> hostHealthMonitor,
            @Nonnull R roles,
            Metrics m,
            String initialHost) {
          return partitionFinder.getAllPartitionNames(resourceName).thenApply(partitions -> {
            try {
              Map<H, BroadcastScatterGatherRequest<H, K>> hostMap = new HashMap<>();
              for (String partitionName: partitions) {
                HostHealthChecked<H> healthChecked = new HostHealthChecked<>(hostHealthMonitor);
                Optional<H> host = Optional.empty();

                List<H> hosts =
                    hostFinder.findHosts(requestMethod, resourceName, partitionName, healthChecked, roles, initialHost);
                if (hosts != null) {
                  List<H> healthyHosts = healthChecked.check(hosts, partitionName);
                  if (healthyHosts != null && !healthyHosts.isEmpty()) {
                    // First host
                    host = Optional.of(healthyHosts.get(0));
                  }
                }

                BroadcastScatterGatherRequest<H, K> request;
                if (host.isPresent()) {
                  request = hostMap.computeIfAbsent(host.get(), h -> {
                    BroadcastScatterGatherRequest<H, K> r =
                        new BroadcastScatterGatherRequest<>(Collections.singletonList(h));
                    scatter.addOnlineRequest(r);
                    return r;
                  });
                } else {
                  request = new BroadcastScatterGatherRequest<>(Collections.emptyList());
                  scatter.addOfflineRequest(request);
                }
                request.addPartitionNameToQuery(partitionName);
              }
              return scatter;
            } catch (RouterException e) {
              throw new CompletionException(e);
            }
          });
        }
      };

  /**
   * One request per partition.
   */
  public static final ScatterGatherMode GROUP_BY_PARTITION = new ScatterGatherMode("GROUP_BY_PARTITION", false) {
    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics metrics) throws RouterException {
      return join(
          scatter(
              scatter,
              requestMethod,
              resourceName,
              sync(partitionFinder),
              hostFinder,
              hostHealthMonitor,
              roles,
              metrics));
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          null);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          initialHost);
    }

    private <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatterBase(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {
      CompletionStage<Map<String, TreeSet<K>>> keyMapStage = CompletableFuture.completedFuture(new HashMap<>());
      for (K key: scatter.getPath().getPartitionKeys()) {
        keyMapStage =
            keyMapStage.thenCombine(partitionFinder.findPartitionName(resourceName, key), (keyMap, partitionName) -> {
              keyMap.compute(partitionName, (name, value) -> {
                if (value == null) {
                  value = new TreeSet<>();
                }
                value.add(key);
                return value;
              });
              return keyMap;
            });
      }
      return keyMapStage.thenApply(keyMap -> {
        try {
          for (Map.Entry<String, TreeSet<K>> entry: keyMap.entrySet()) {
            HostHealthChecked<H> healthChecked = new HostHealthChecked<>(hostHealthMonitor);
            List<H> hosts =
                hostFinder.findHosts(requestMethod, resourceName, entry.getKey(), healthChecked, roles, initialHost);
            ScatterGatherRequest<H, K> request;
            if ((hosts = healthChecked.check(hosts, entry.getKey())).isEmpty()) { // SUPPRESS CHECKSTYLE InnerAssignment
              request = new ScatterGatherRequest<>(Collections.emptyList(), entry.getValue());
              scatter.addOfflineRequest(request);
            } else {
              request = new ScatterGatherRequest<>(hosts, entry.getValue());
              scatter.addOnlineRequest(request);
            }
          }
          return scatter;
        } catch (RouterException e) {
          throw new CompletionException(e);
        }
      });
    }
  };

  /**
   * Sends request for first host for each partition in request, grouped by host
   */
  public static final ScatterGatherMode GROUP_BY_PRIMARY_HOST = new GroupByHost("GROUP_BY_PRIMARY_HOST") {
    @Override
    <H> void mergeHosts(Stream<H> hosts, Consumer<H> consumer) {
      hosts.findFirst().ifPresent(consumer);
    }
  };

  /**
   * Sends request for minimal group of hosts. When multiple hosts can serve the same partition,
   * try to use maximal munch to select the smallest number of hosts to handle the partitions.
   */
  public static final ScatterGatherMode GROUP_BY_GREEDY_HOST = new GroupByHost("GROUP_BY_GREEDY_HOST") {
    @Override
    <H> void mergeHosts(Stream<H> hosts, Consumer<H> consumer) {
      hosts.forEach(consumer);
    }
  };

  static class HostHealthChecked<H> implements HostHealthMonitor<H> {
    private final HostHealthMonitor<H> _monitor;
    private boolean _checked;

    private HostHealthChecked(HostHealthMonitor<H> monitor) {
      _monitor = monitor;
    }

    @Override
    public boolean isHostHealthy(@Nonnull H hostName, @Nonnull String partitionName) {
      _checked = true;
      return _monitor.isHostHealthy(hostName, partitionName);
    }

    private List<H> check(List<H> hosts, String partitionName) {
      if (hosts == null || hosts.isEmpty()) {
        return Collections.emptyList();
      }

      if (_checked) {
        return hosts;
      } else {
        List<H> healthyHosts = new ArrayList<>(hosts.size());
        for (H host: hosts) {
          if (_monitor.isHostHealthy(host, partitionName)) {
            healthyHosts.add(host);
          }
        }
        return healthyHosts;
      }
    }
  }

  abstract static class GroupByHost extends ScatterGatherMode {
    GroupByHost(@Nonnull String name) {
      super(name, false);
    }

    abstract <H> void mergeHosts(Stream<H> hosts, Consumer<H> consumer);

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics metrics) throws RouterException {
      return join(
          scatter(
              scatter,
              requestMethod,
              resourceName,
              sync(partitionFinder),
              hostFinder,
              hostHealthMonitor,
              roles,
              metrics));
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          null);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {
      return scatterBase(
          scatter,
          requestMethod,
          resourceName,
          partitionFinder,
          hostFinder,
          hostHealthMonitor,
          roles,
          m,
          initialHost);
    }

    @Nonnull
    private <H, P extends ResourcePath<K>, K, R> CompletionStage<Scatter<H, P, K>> scatterBase(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull AsyncPartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics m,
        String initialHost) {

      final class HostInfo implements Comparable<HostInfo> {
        final H host;
        String hostString;
        final Set<String> partitions;
        int count = 1;

        private HostInfo(H host, String partition) {
          this.host = host;
          partitions = new HashSet<>(Collections.singleton(partition));
        }

        private HostInfo merge(HostInfo other) {
          if (other.partitions != partitions) {
            partitions.addAll(other.partitions);
          }
          count += other.count;
          return this;
        }

        @Override
        public String toString() {
          if (hostString == null) {
            hostString = host.toString();
          }
          return hostString;
        }

        @Override
        public int compareTo(HostInfo o) {
          int compare = Integer.compare(count, o.count);
          return compare == 0 ? String.CASE_INSENSITIVE_ORDER.compare(toString(), o.toString()) : compare;
        }

        @Override
        public boolean equals(Object o) {
          if (this == o) {
            return true;
          }
          if (o == null || getClass() != o.getClass()) {
            return false;
          }
          HostInfo hostInfo = (HostInfo) o;
          return count == hostInfo.count && toString().equals(hostInfo.toString());
        }

        @Override
        public int hashCode() {
          return Objects.hash(toString(), count);
        }
      }

      CompletionStage<Pair<Map<H, HostInfo>, Map<String, Pair<List<H>, List<K>>>>> hostMapAndPartitionsStage =
          CompletableFuture.completedFuture(Pair.make(new HashMap<>(), new HashMap<>()));
      for (K key: scatter.getPath().getPartitionKeys()) {
        hostMapAndPartitionsStage = hostMapAndPartitionsStage.thenCombine(
            partitionFinder.findPartitionName(resourceName, key),
            (hostMapAndPartitions, partitionName) -> {
              hostMapAndPartitions.getSecond().computeIfAbsent(partitionName, partition -> {
                try {
                  HostHealthChecked<H> healthChecked = new HostHealthChecked<>(hostHealthMonitor);
                  List<H> hosts =
                      hostFinder.findHosts(requestMethod, resourceName, partition, healthChecked, roles, initialHost);
                  if ((hosts = healthChecked.check(hosts, partitionName)).isEmpty()) { // SUPPRESS CHECKSTYLE
                                                                                       // InnerAssignment
                    hosts = Collections.emptyList();
                  } else {
                    mergeHosts(
                        hosts.stream(),
                        host -> hostMapAndPartitions.getFirst()
                            .merge(host, new HostInfo(host, partition), HostInfo::merge));
                  }
                  return Pair.make(hosts, new ArrayList<>());
                } catch (RouterException e) {
                  LOG.debug("HostFinder exception", e);
                  throw new CompletionException(e);
                }
              }).getSecond().add(key);
              return hostMapAndPartitions;
            });
      }
      return hostMapAndPartitionsStage
          .thenApply(
              hostMapAndPartitions -> Pair
                  .make(new LinkedList<>(hostMapAndPartitions.getFirst().values()), hostMapAndPartitions.getSecond()))
          .thenApply(hostListAndPartitions -> {
            List<HostInfo> hostList = hostListAndPartitions.getFirst();
            Map<String, Pair<List<H>, List<K>>> partitions = hostListAndPartitions.getSecond();

            while (!partitions.isEmpty() && !hostList.isEmpty()) {
              HostInfo next = Collections.max(hostList);
              hostList.remove(next);

              TreeSet<K> keys = next.partitions.stream()
                  .flatMap(
                      partition -> Optional.ofNullable(partitions.remove(partition))
                          .map(Pair::getSecond)
                          .map(Collection::stream)
                          .orElseGet(Stream::empty))
                  .collect(Collectors.toCollection(TreeSet::new));

              scatter.addOnlineRequest(new ScatterGatherRequest<>(Collections.singletonList(next.host), keys));

              hostList.removeIf(hostInfo -> {
                next.partitions.stream().filter(hostInfo.partitions::remove).forEach(partition -> hostInfo.count--);
                return hostInfo.count < 1 || hostInfo.partitions.isEmpty();
              });
            }

            partitions.forEach(
                (partition, pair) -> scatter.addOfflineRequest(
                    new ScatterGatherRequest<H, K>(Collections.emptyList(), new TreeSet<>(pair.getSecond()))));

            return scatter;
          });
    }
  }
}
