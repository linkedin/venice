package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;


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
  private ClientConfig routerClientConfig;
  private ICProvider icProvider;
  private SupersetSchemaGenerator externalSupersetSchemaGenerator;
  private PubSubClientsFactory pubSubClientsFactory;

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
  }

  public static class Builder {
    private List<VeniceProperties> propertiesList;
    private MetricsRepository metricsRepository;
    private List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
    private DynamicAccessController accessController;
    private AuthorizerService authorizerService;
    private D2Client d2Client;
    private ClientConfig routerClientConfig;
    private ICProvider icProvider;
    private SupersetSchemaGenerator externalSupersetSchemaGenerator;
    private PubSubClientsFactory pubSubClientsFactory;

    private boolean isMetricsRepositorySet;
    private boolean isServiceDiscoveryAnnouncerSet;

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

    private void addDefaultValues() {
      if (metricsRepository == null && !isMetricsRepositorySet) {
        metricsRepository = TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME);
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
