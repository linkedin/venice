package com.linkedin.venice.controller;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Dependencies for VeniceControllerRequestHandler
 */
public class ControllerRequestHandlerDependencies {
  private static final Logger LOGGER = LogManager.getLogger(ControllerRequestHandlerDependencies.class);
  private final Admin admin;
  private final boolean enforceSSL;
  private final boolean sslEnabled;
  private final boolean checkReadMethodForKafka;
  private final SSLConfig sslConfig;
  private final DynamicAccessController accessController;
  private final List<ControllerRoute> disabledRoutes;
  private final Set<String> clusters;
  private final boolean disableParentRequestTopicForStreamPushes;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;
  private final VeniceProperties veniceProperties;
  private final VeniceControllerAccessManager controllerAccessManager;

  private ControllerRequestHandlerDependencies(Builder builder) {
    this.admin = builder.admin;
    this.enforceSSL = builder.enforceSSL;
    this.sslEnabled = builder.sslEnabled;
    this.checkReadMethodForKafka = builder.checkReadMethodForKafka;
    this.sslConfig = builder.sslConfig;
    this.accessController = builder.accessController;
    this.disabledRoutes = builder.disabledRoutes;
    this.clusters = builder.clusters;
    this.disableParentRequestTopicForStreamPushes = builder.disableParentRequestTopicForStreamPushes;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.metricsRepository = builder.metricsRepository;
    this.veniceProperties = builder.veniceProperties;
    this.controllerAccessManager = builder.controllerAccessManager;
  }

  public Admin getAdmin() {
    return admin;
  }

  public Set<String> getClusters() {
    return clusters;
  }

  public boolean isEnforceSSL() {
    return enforceSSL;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  public boolean isCheckReadMethodForKafka() {
    return checkReadMethodForKafka;
  }

  public SSLConfig getSslConfig() {
    return sslConfig;
  }

  public DynamicAccessController getAccessController() {
    return accessController;
  }

  public List<ControllerRoute> getDisabledRoutes() {
    return disabledRoutes;
  }

  public boolean isDisableParentRequestTopicForStreamPushes() {
    return disableParentRequestTopicForStreamPushes;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public VeniceControllerAccessManager getControllerAccessManager() {
    return controllerAccessManager;
  }

  // Builder class for VeniceControllerRequestHandlerDependencies
  public static class Builder {
    private Admin admin;
    private boolean enforceSSL;
    private boolean sslEnabled;
    private boolean checkReadMethodForKafka;
    private SSLConfig sslConfig;
    private DynamicAccessController accessController;
    private List<ControllerRoute> disabledRoutes;
    private Set<String> clusters;
    private boolean disableParentRequestTopicForStreamPushes;
    private PubSubTopicRepository pubSubTopicRepository;
    private MetricsRepository metricsRepository;
    private VeniceProperties veniceProperties;
    private VeniceControllerAccessManager controllerAccessManager;

    public Builder setAdmin(Admin admin) {
      this.admin = admin;
      return this;
    }

    public Builder setClusters(Set<String> clusters) {
      this.clusters = clusters;
      return this;
    }

    public Builder setEnforceSSL(boolean enforceSSL) {
      this.enforceSSL = enforceSSL;
      return this;
    }

    public Builder setSslEnabled(boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      return this;
    }

    public Builder setCheckReadMethodForKafka(boolean checkReadMethodForKafka) {
      this.checkReadMethodForKafka = checkReadMethodForKafka;
      return this;
    }

    public Builder setSslConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    public Builder setAccessController(DynamicAccessController accessController) {
      this.accessController = accessController;
      return this;
    }

    public Builder setDisabledRoutes(List<ControllerRoute> disabledRoutes) {
      this.disabledRoutes = disabledRoutes;
      return this;
    }

    public Builder setDisableParentRequestTopicForStreamPushes(boolean disableParentRequestTopicForStreamPushes) {
      this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    private void verifyAndAddDefaults() {
      if (admin == null) {
        throw new IllegalArgumentException("admin is mandatory dependencies for VeniceControllerRequestHandler");
      }
      if (pubSubTopicRepository == null) {
        pubSubTopicRepository = new PubSubTopicRepository();
      }
      if (disabledRoutes == null) {
        disabledRoutes = Collections.emptyList();
      }
      if (sslEnabled && sslConfig == null) {
        throw new IllegalArgumentException("sslConfig is mandatory when sslEnabled is true");
      }
      if (accessController == null || !sslEnabled) {
        String reason = (accessController == null ? "access controller is not configured" : "")
            + (accessController == null && !sslEnabled ? " and " : "") + (!sslEnabled ? "SSL is disabled" : "");
        LOGGER.info("Defaulting to NoOpDynamicAccessController because {}.", reason);
        accessController = NoOpDynamicAccessController.INSTANCE;
      }
      controllerAccessManager = new VeniceControllerAccessManager(accessController);
    }

    public ControllerRequestHandlerDependencies build() {
      verifyAndAddDefaults();
      return new ControllerRequestHandlerDependencies(this);
    }
  }
}
