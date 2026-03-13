package com.linkedin.venice.samza;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriterHook;
import java.util.Objects;
import java.util.Optional;
import org.apache.samza.config.Config;


/**
 * Configuration options for constructing a {@link VeniceSystemProducer}.
 * Use the {@link Builder} to create instances.
 *
 * Three mutually exclusive connection modes are supported:
 * - ZK-based: set {@code veniceChildD2ZkHost} and {@code primaryControllerColoD2ZKHost}
 * - D2Client-based: set both {@code providedChildColoD2Client} and {@code providedPrimaryControllerColoD2Client}
 * - Discovery-URL-based: set {@code discoveryUrl}
 */
public class VeniceSystemProducerConfig {
  // Required
  private final String storeName;
  private final Version.PushType pushType;
  private final String samzaJobId;
  private final String runningFabric;
  private final boolean verifyLatestProtocolPresent;

  // ZK-based connection
  private final String veniceChildD2ZkHost;
  private final String primaryControllerColoD2ZKHost;
  private final String primaryControllerD2ServiceName;

  // D2Client-based connection
  private final D2Client providedChildColoD2Client;
  private final D2Client providedPrimaryControllerColoD2Client;

  // Discovery-URL-based connection
  private final String discoveryUrl;

  // Optional
  private final VeniceSystemFactory factory;
  private final Optional<SSLFactory> sslFactory;
  private final Optional<String> partitioners;
  private final Time time;
  private final VeniceWriterHook writerHook;
  private final Config samzaConfig;
  private final String routerUrl;

  private VeniceSystemProducerConfig(Builder builder) {
    this.storeName = builder.storeName;
    this.pushType = builder.pushType;
    this.samzaJobId = builder.samzaJobId;
    this.runningFabric = builder.runningFabric;
    this.verifyLatestProtocolPresent = builder.verifyLatestProtocolPresent;
    this.veniceChildD2ZkHost = builder.veniceChildD2ZkHost;
    this.primaryControllerColoD2ZKHost = builder.primaryControllerColoD2ZKHost;
    this.primaryControllerD2ServiceName = builder.primaryControllerD2ServiceName;
    this.providedChildColoD2Client = builder.providedChildColoD2Client;
    this.providedPrimaryControllerColoD2Client = builder.providedPrimaryControllerColoD2Client;
    this.discoveryUrl = builder.discoveryUrl;
    this.factory = builder.factory;
    this.sslFactory = builder.sslFactory;
    this.partitioners = builder.partitioners;
    this.time = builder.time;
    this.writerHook = builder.writerHook;
    this.samzaConfig = builder.samzaConfig;
    this.routerUrl = builder.routerUrl;
  }

  public String getStoreName() {
    return storeName;
  }

  public Version.PushType getPushType() {
    return pushType;
  }

  public String getSamzaJobId() {
    return samzaJobId;
  }

  public String getRunningFabric() {
    return runningFabric;
  }

  public boolean isVerifyLatestProtocolPresent() {
    return verifyLatestProtocolPresent;
  }

  public String getVeniceChildD2ZkHost() {
    return veniceChildD2ZkHost;
  }

  public String getPrimaryControllerColoD2ZKHost() {
    return primaryControllerColoD2ZKHost;
  }

  public String getPrimaryControllerD2ServiceName() {
    return primaryControllerD2ServiceName;
  }

  public D2Client getProvidedChildColoD2Client() {
    return providedChildColoD2Client;
  }

  public D2Client getProvidedPrimaryControllerColoD2Client() {
    return providedPrimaryControllerColoD2Client;
  }

  public String getDiscoveryUrl() {
    return discoveryUrl;
  }

  public VeniceSystemFactory getFactory() {
    return factory;
  }

  public Optional<SSLFactory> getSslFactory() {
    return sslFactory;
  }

  public Optional<String> getPartitioners() {
    return partitioners;
  }

  public Time getTime() {
    return time;
  }

  public VeniceWriterHook getWriterHook() {
    return writerHook;
  }

  public Config getSamzaConfig() {
    return samzaConfig;
  }

  public String getRouterUrl() {
    return routerUrl;
  }

  public static class Builder {
    private String storeName;
    private Version.PushType pushType;
    private String samzaJobId;
    private String runningFabric;
    private boolean verifyLatestProtocolPresent;
    private String veniceChildD2ZkHost;
    private String primaryControllerColoD2ZKHost;
    private String primaryControllerD2ServiceName;
    private D2Client providedChildColoD2Client;
    private D2Client providedPrimaryControllerColoD2Client;
    private String discoveryUrl;
    private VeniceSystemFactory factory;
    private Optional<SSLFactory> sslFactory = Optional.empty();
    private Optional<String> partitioners = Optional.empty();
    private Time time = SystemTime.INSTANCE;
    private VeniceWriterHook writerHook;
    private Config samzaConfig;
    private String routerUrl;

    /** @param storeName the Venice store to write to (required) */
    public Builder setStoreName(String storeName) {
      this.storeName = storeName;
      return this;
    }

    /** @param pushType the {@link Version.PushType} to use for writing (required) */
    public Builder setPushType(Version.PushType pushType) {
      this.pushType = pushType;
      return this;
    }

    /** @param samzaJobId unique ID for jobs that may concurrently write to the same store */
    public Builder setSamzaJobId(String samzaJobId) {
      this.samzaJobId = samzaJobId;
      return this;
    }

    /** @param runningFabric the colo where the job is running, used to determine the optimal write destination */
    public Builder setRunningFabric(String runningFabric) {
      this.runningFabric = runningFabric;
      return this;
    }

    /** @param verifyLatestProtocolPresent whether to verify that runtime protocol versions are valid in Venice backend */
    public Builder setVerifyLatestProtocolPresent(boolean verifyLatestProtocolPresent) {
      this.verifyLatestProtocolPresent = verifyLatestProtocolPresent;
      return this;
    }

    /** @param veniceChildD2ZkHost D2 ZK address where components in the child colo announce themselves */
    public Builder setVeniceChildD2ZkHost(String veniceChildD2ZkHost) {
      this.veniceChildD2ZkHost = veniceChildD2ZkHost;
      return this;
    }

    /** @param primaryControllerColoD2ZKHost D2 ZK address of the colo where the primary controller resides */
    public Builder setPrimaryControllerColoD2ZKHost(String primaryControllerColoD2ZKHost) {
      this.primaryControllerColoD2ZKHost = primaryControllerColoD2ZKHost;
      return this;
    }

    /** @param primaryControllerD2ServiceName the D2 service name the primary controller uses to announce itself */
    public Builder setPrimaryControllerD2ServiceName(String primaryControllerD2ServiceName) {
      this.primaryControllerD2ServiceName = primaryControllerD2ServiceName;
      return this;
    }

    /** @param providedChildColoD2Client pre-configured D2Client for the child colo (must be paired with primary) */
    public Builder setProvidedChildColoD2Client(D2Client providedChildColoD2Client) {
      this.providedChildColoD2Client = providedChildColoD2Client;
      return this;
    }

    /** @param providedPrimaryControllerColoD2Client pre-configured D2Client for the primary controller colo (must be paired with child) */
    public Builder setProvidedPrimaryControllerColoD2Client(D2Client providedPrimaryControllerColoD2Client) {
      this.providedPrimaryControllerColoD2Client = providedPrimaryControllerColoD2Client;
      return this;
    }

    /** @param discoveryUrl controller discovery URL (mutually exclusive with ZK hosts and D2 clients) */
    public Builder setDiscoveryUrl(String discoveryUrl) {
      this.discoveryUrl = discoveryUrl;
      return this;
    }

    /** @param factory the {@link VeniceSystemFactory} that created this producer */
    public Builder setFactory(VeniceSystemFactory factory) {
      this.factory = factory;
      return this;
    }

    /** @param sslFactory SSL factory for secure communication, or null to disable SSL */
    public Builder setSslFactory(SSLFactory sslFactory) {
      this.sslFactory = Optional.ofNullable(sslFactory);
      return this;
    }

    /** @param partitioners comma-separated list of supported partitioner class names, or null for default */
    public Builder setPartitioners(String partitioners) {
      this.partitioners = Optional.ofNullable(partitioners);
      return this;
    }

    /** @param time time provider, defaults to {@link SystemTime#INSTANCE} */
    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    /** @param writerHook callback invoked before each produce for backpressure/quota throttling */
    public Builder setWriterHook(VeniceWriterHook writerHook) {
      this.writerHook = writerHook;
      return this;
    }

    /** @param samzaConfig the Samza job {@link Config}, applied as additional writer properties */
    public Builder setSamzaConfig(Config samzaConfig) {
      this.samzaConfig = samzaConfig;
      return this;
    }

    /** @param routerUrl Venice router URL for schema verification */
    public Builder setRouterUrl(String routerUrl) {
      this.routerUrl = routerUrl;
      return this;
    }

    public VeniceSystemProducerConfig build() {
      Objects.requireNonNull(storeName, "storeName cannot be null");
      Objects.requireNonNull(pushType, "pushType cannot be null");

      boolean hasDiscoveryUrl = discoveryUrl != null;
      boolean hasD2Clients = providedChildColoD2Client != null || providedPrimaryControllerColoD2Client != null;
      boolean hasZkHosts = veniceChildD2ZkHost != null || primaryControllerColoD2ZKHost != null;

      if (hasDiscoveryUrl && (hasD2Clients || hasZkHosts)) {
        throw new IllegalStateException(
            "Cannot set both discoveryUrl and ZK hosts or D2 clients. Use one connection mode.");
      }
      if (hasD2Clients && hasZkHosts) {
        throw new IllegalStateException("Cannot set both D2 clients and ZK hosts. Use one connection mode.");
      }
      if (hasDiscoveryUrl && discoveryUrl.trim().isEmpty()) {
        throw new IllegalStateException("Discovery URL must not be empty");
      }
      if (hasD2Clients && (providedChildColoD2Client == null || providedPrimaryControllerColoD2Client == null)) {
        throw new IllegalStateException(
            "Both childColoD2Client and primaryControllerColoD2Client must be set together");
      }
      return new VeniceSystemProducerConfig(this);
    }
  }
}
