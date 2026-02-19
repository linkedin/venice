package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * VeniceControllerContext contains dependencies required by {@link VeniceController}
 */
public class VeniceControllerContext {
  private List<VeniceProperties> propertiesList;
  private MetricsRepository metricsRepository;
  private List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private DynamicAccessController accessController;
  private AuthorizerService authorizerService;
  private D2Client d2Client;

  private Map<String, D2Client> d2Clients;
  private ClientConfig routerClientConfig;
  private ICProvider icProvider;
  private SupersetSchemaGenerator externalSupersetSchemaGenerator;
  private PubSubClientsFactory pubSubClientsFactory;
  private List<VeniceVersionLifecycleEventListener> versionLifecycleEventListeners;
  private ExternalETLService externalETLService;

  public List<VeniceProperties> getPropertiesList() {
    return propertiesList;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public List<ServiceDiscoveryAnnouncer> getServiceDiscoveryAnnouncers() {
    return serviceDiscoveryAnnouncers;
  }

  public DynamicAccessController getAccessController() {
    return accessController;
  }

  public AuthorizerService getAuthorizerService() {
    return authorizerService;
  }

  public D2Client getD2Client() {
    return d2Client;
  }

  public Map<String, D2Client> getD2Clients() {
    return d2Clients;
  }

  public ClientConfig getRouterClientConfig() {
    return routerClientConfig;
  }

  public ICProvider getIcProvider() {
    return icProvider;
  }

  public SupersetSchemaGenerator getExternalSupersetSchemaGenerator() {
    return externalSupersetSchemaGenerator;
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return pubSubClientsFactory;
  }

  public List<VeniceVersionLifecycleEventListener> getVersionLifecycleEventListeners() {
    return versionLifecycleEventListeners;
  }

  public ExternalETLService getExternalETLService() {
    return externalETLService;
  }

  public VeniceControllerContext(Builder builder) {
    this.propertiesList = builder.propertiesList;
    this.metricsRepository = builder.metricsRepository;
    this.serviceDiscoveryAnnouncers = builder.serviceDiscoveryAnnouncers;
    this.accessController = builder.accessController;
    this.authorizerService = builder.authorizerService;
    this.d2Client = builder.d2Client;
    this.routerClientConfig = builder.routerClientConfig;
    this.icProvider = builder.icProvider;
    this.externalSupersetSchemaGenerator = builder.externalSupersetSchemaGenerator;
    this.pubSubClientsFactory = builder.pubSubClientsFactory;
    this.d2Clients = builder.d2Clients;
    this.versionLifecycleEventListeners = builder.versionLifecycleEventListeners;
    this.externalETLService = builder.externalETLService;
  }

  public static class Builder {
    private List<VeniceProperties> propertiesList;
    private MetricsRepository metricsRepository;
    private List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
    private DynamicAccessController accessController;
    private AuthorizerService authorizerService;
    private D2Client d2Client;
    private Map<String, D2Client> d2Clients;
    private ClientConfig routerClientConfig;
    private ICProvider icProvider;
    private SupersetSchemaGenerator externalSupersetSchemaGenerator;
    private PubSubClientsFactory pubSubClientsFactory;

    private boolean isMetricsRepositorySet;
    private boolean isServiceDiscoveryAnnouncerSet;
    private List<VeniceVersionLifecycleEventListener> versionLifecycleEventListeners;
    private ExternalETLService externalETLService;

    public Builder setPropertiesList(List<VeniceProperties> propertiesList) {
      this.propertiesList = propertiesList;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.isMetricsRepositorySet = true;
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setServiceDiscoveryAnnouncers(List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers) {
      this.isServiceDiscoveryAnnouncerSet = true;
      this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
      return this;
    }

    public Builder setAccessController(DynamicAccessController accessController) {
      this.accessController = accessController;
      return this;
    }

    public Builder setAuthorizerService(AuthorizerService authorizerService) {
      this.authorizerService = authorizerService;
      return this;
    }

    public Builder setD2Client(D2Client d2Client) {
      this.d2Client = d2Client;
      return this;
    }

    public Builder setRouterClientConfig(ClientConfig routerClientConfig) {
      this.routerClientConfig = routerClientConfig;
      return this;
    }

    public Builder setIcProvider(ICProvider icProvider) {
      this.icProvider = icProvider;
      return this;
    }

    public Builder setExternalSupersetSchemaGenerator(SupersetSchemaGenerator externalSupersetSchemaGenerator) {
      this.externalSupersetSchemaGenerator = externalSupersetSchemaGenerator;
      return this;
    }

    public Builder setPubSubClientsFactory(PubSubClientsFactory pubSubClientsFactory) {
      this.pubSubClientsFactory = pubSubClientsFactory;
      return this;
    }

    // Set D2 Client
    public Builder setD2Clients(Map<String, D2Client> d2Clients) {
      this.d2Clients = d2Clients;
      return this;
    }

    public Builder setVersionLifecycleEventListeners(
        List<VeniceVersionLifecycleEventListener> versionLifecycleEventListeners) {
      this.versionLifecycleEventListeners = versionLifecycleEventListeners;
      return this;
    }

    public Builder setExternalETLService(ExternalETLService externalETLService) {
      this.externalETLService = externalETLService;
      return this;
    }

    private void addDefaultValues() {
      if (metricsRepository == null && !isMetricsRepositorySet) {

        metricsRepository = VeniceMetricsRepository.getVeniceMetricsRepository(
            CONTROLLER_SERVICE_NAME,
            CONTROLLER_SERVICE_METRIC_PREFIX,
            CONTROLLER_SERVICE_METRIC_ENTITIES,
            (propertiesList == null || propertiesList.isEmpty())
                ? new VeniceProperties().getAsMap()
                : propertiesList.get(0).getAsMap());
        // TODO OTel: today, this gets the properties of the first cluster. This is not ideal.
        // We need to figure out how to build a common controller-specific config/properties list
      }
      if (serviceDiscoveryAnnouncers == null && !isServiceDiscoveryAnnouncerSet) {
        serviceDiscoveryAnnouncers = Collections.emptyList();
      }
    }

    public VeniceControllerContext build() {
      addDefaultValues();
      return new VeniceControllerContext(this);
    }

  }

}
