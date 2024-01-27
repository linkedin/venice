package com.linkedin.alpini.router.api;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/8/18.
 */
public class TestScatterGatherHelperBuilder {
  enum State {
    LEADER, FOLLOWER
  }

  @Test(groups = "unit")
  public void testBuilder() {

    class Path implements ResourcePath<String> {
      private final String resource;
      private final List<String> partitionKey;
      private final String location;

      public Path(String resource, String partitionKey, String location) {
        this(resource, Collections.singletonList(Objects.requireNonNull(partitionKey, "partitionKey")), location);
      }

      public Path(String resource, List<String> partitionKey, String location) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey");
        this.location = location;
      }

      private String getRemainder() {
        return "";
      }

      @Nonnull
      @Override
      public String getLocation() {
        return location;
      }

      @Nonnull
      @Override
      public Collection<String> getPartitionKeys() {
        return partitionKey;
      }

      @Nonnull
      @Override
      public String getResourceName() {
        return resource;
      }
    }

    class Parser implements com.linkedin.alpini.router.api.ResourcePathParser<Path, String> {
      @Nonnull
      @Override
      public Path parseResourceUri(@Nonnull String uri) throws RouterException {
        return new Path(uri, "", "/");
      }

      @Nonnull
      @Override
      public Path substitutePartitionKey(@Nonnull Path path, String s) {
        return path;
      }

      @Nonnull
      @Override
      public Path substitutePartitionKey(@Nonnull Path path, @Nonnull Collection<String> s) {
        return path;
      }
    }

    class PartitionFinder implements com.linkedin.alpini.router.api.PartitionFinder<String> {
      @Nonnull
      @Override
      public String findPartitionName(@Nonnull String resourceName, @Nonnull String partitionKey)
          throws RouterException {
        return "shard_0";
      }

      @Nonnull
      @Override
      public List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException {
        return Collections.singletonList("shard_0");
      }

      @Override
      public int getNumPartitions(@Nonnull String resourceName) throws RouterException {
        return 1;
      }

      @Override
      public int findPartitionNumber(
          @Nonnull String partitionKey,
          int numPartitions,
          String storeName,
          int versionNumber) throws RouterException {
        return 0;
      }
    }

    class HostFinder implements com.linkedin.alpini.router.api.HostFinder<InetSocketAddress, List<List<State>>> {
      private final List<InetSocketAddress> hosts;

      private HostFinder() {
        hosts = Arrays.asList(new InetSocketAddress("127.0.0.1", 10000), new InetSocketAddress("127.0.0.1", 10001));
      }

      @Nonnull
      @Override
      public List<InetSocketAddress> findHosts(
          @Nonnull String requestMethod,
          @Nonnull String resourceName,
          @Nonnull String partitionName,
          @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
          @Nonnull List<List<State>> roles) throws RouterException {
        return Collections
            .singletonList(hosts.get((int) ((0xffffffffL & (long) partitionName.hashCode()) % hosts.size())));
      }
    }

    Assert.assertNotNull(
        ScatterGatherHelper.builder()
            .roleFinder(
                (method, httpHeaders) -> Collections.unmodifiableList(
                    Arrays.asList(Collections.singletonList(State.LEADER), Collections.singletonList(State.FOLLOWER))))
            .pathParser(new Parser())
            .partitionFinder(new PartitionFinder())
            .hostFinder(new HostFinder())
            .dispatchHandler(
                (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {})
            .build());
  }
}
