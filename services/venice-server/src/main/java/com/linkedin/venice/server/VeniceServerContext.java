package com.linkedin.venice.server;

import static com.linkedin.venice.server.VeniceServer.SERVER_SERVICE_NAME;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;


/**
 * VeniceServerContext contains dependencies required by VeniceServer
 */
public class VeniceServerContext {
  private final VeniceConfigLoader veniceConfigLoader;
  private final MetricsRepository metricsRepository;
  private final SSLFactory sslFactory;
  private final StaticAccessController routerAccessController;
  private final DynamicAccessController storeAccessController;
  private final ClientConfig clientConfigForConsumer;
  private final ICProvider icProvider;
  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;

  public VeniceConfigLoader getVeniceConfigLoader() {
    return veniceConfigLoader;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  public StaticAccessController getRouterAccessController() {
    return routerAccessController;
  }

  public DynamicAccessController getStoreAccessController() {
    return storeAccessController;
  }

  public ClientConfig getClientConfigForConsumer() {
    return clientConfigForConsumer;
  }

  public ICProvider getIcProvider() {
    return icProvider;
  }

  public List<ServiceDiscoveryAnnouncer> getServiceDiscoveryAnnouncers() {
    return serviceDiscoveryAnnouncers;
  }

  private VeniceServerContext(Builder builder) {
    this.veniceConfigLoader = builder.veniceConfigLoader;
    this.metricsRepository = builder.metricsRepository;
    this.sslFactory = builder.sslFactory;
    this.routerAccessController = builder.routerAccessController;
    this.storeAccessController = builder.storeAccessController;
    this.clientConfigForConsumer = builder.clientConfigForConsumer;
    this.icProvider = builder.icProvider;
    this.serviceDiscoveryAnnouncers = builder.serviceDiscoveryAnnouncers;
  }

  public static class Builder {
    private VeniceConfigLoader veniceConfigLoader;
    private MetricsRepository metricsRepository;
    private boolean isMetricsRepositorySet;
    private SSLFactory sslFactory;
    private StaticAccessController routerAccessController;
    private DynamicAccessController storeAccessController;
    private ClientConfig clientConfigForConsumer;
    private ICProvider icProvider;
    private List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
    private boolean isServiceDiscoveryAnnouncerSet;

    public Builder setVeniceConfigLoader(VeniceConfigLoader veniceConfigLoader) {
      this.veniceConfigLoader = veniceConfigLoader;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.isMetricsRepositorySet = true;
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setSslFactory(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
      return this;
    }

    public Builder setRouterAccessController(StaticAccessController routerAccessController) {
      this.routerAccessController = routerAccessController;
      return this;
    }

    public Builder setStoreAccessController(DynamicAccessController storeAccessController) {
      this.storeAccessController = storeAccessController;
      return this;
    }

    public Builder setClientConfigForConsumer(ClientConfig clientConfigForConsumer) {
      this.clientConfigForConsumer = clientConfigForConsumer;
      return this;
    }

    public Builder setIcProvider(ICProvider icProvider) {
      this.icProvider = icProvider;
      return this;
    }

    public Builder setServiceDiscoveryAnnouncers(List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers) {
      this.isServiceDiscoveryAnnouncerSet = true;
      this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
      return this;
    }

    private void addDefaultValues() {
      if (metricsRepository == null && !isMetricsRepositorySet) {
        metricsRepository = TehutiUtils.getMetricsRepository(SERVER_SERVICE_NAME);
      }
      if (serviceDiscoveryAnnouncers == null && !isServiceDiscoveryAnnouncerSet) {
        serviceDiscoveryAnnouncers = Collections.emptyList();
      }
    }

    public VeniceServerContext build() {
      addDefaultValues();
      return new VeniceServerContext(this);
    }
  }
}
