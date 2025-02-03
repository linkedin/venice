package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_CONTROLLERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_ROUTERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_SERVERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_STORAGE_NODES;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import java.util.Properties;


public class VeniceMultiRegionClusterCreateOptions {
  private final int numberOfRegions;
  private final int numberOfClusters;
  private final int numberOfParentControllers;
  private final int numberOfChildControllers;
  private final int numberOfServers;
  private final int numberOfRouters;
  private final int replicationFactor;
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;
  private final boolean forkServer;
  private final Properties parentControllerProperties;
  private final Properties childControllerProperties;
  private final Properties serverProperties;
  private final AuthorizerService parentAuthorizerService;
  private final String parentVeniceZkBasePath;
  private final String childVeniceZkBasePath;
  private final DynamicAccessController accessController;

  public int getNumberOfRegions() {
    return numberOfRegions;
  }

  public int getNumberOfClusters() {
    return numberOfClusters;
  }

  public int getNumberOfParentControllers() {
    return numberOfParentControllers;
  }

  public int getNumberOfChildControllers() {
    return numberOfChildControllers;
  }

  public int getNumberOfServers() {
    return numberOfServers;
  }

  public int getNumberOfRouters() {
    return numberOfRouters;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public boolean isSslToStorageNodes() {
    return sslToStorageNodes;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
  }

  public boolean isForkServer() {
    return forkServer;
  }

  public Properties getParentControllerProperties() {
    return parentControllerProperties;
  }

  public Properties getChildControllerProperties() {
    return childControllerProperties;
  }

  public Properties getServerProperties() {
    return serverProperties;
  }

  public AuthorizerService getParentAuthorizerService() {
    return parentAuthorizerService;
  }

  public String getParentVeniceZkBasePath() {
    return parentVeniceZkBasePath;
  }

  public String getChildVeniceZkBasePath() {
    return childVeniceZkBasePath;
  }

  public DynamicAccessController getAccessController() {
    return accessController;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("VeniceMultiClusterCreateOptions - ")
        .append("numberOfRegions:")
        .append(numberOfRegions)
        .append(", ")
        .append("clusters:")
        .append(numberOfClusters)
        .append(", ")
        .append("parent controllers:")
        .append(numberOfParentControllers)
        .append(", ")
        .append("child controllers:")
        .append(numberOfChildControllers)
        .append(", ")
        .append("servers:")
        .append(numberOfServers)
        .append(", ")
        .append("routers:")
        .append(numberOfRouters)
        .append(", ")
        .append("replicationFactor:")
        .append(replicationFactor)
        .append(", ")
        .append("sslToStorageNodes:")
        .append(sslToStorageNodes)
        .append(", ")
        .append("sslToKafka:")
        .append(sslToKafka)
        .append(", ")
        .append("forkServer:")
        .append(forkServer)
        .append(", ")
        .append("childControllerProperties:")
        .append(childControllerProperties)
        .append(", ")
        .append("parentControllerProperties:")
        .append(parentControllerProperties)
        .append(", ")
        .append("serverProperties:")
        .append(serverProperties)
        .append(", ")
        .append("parentAuthorizerService:")
        .append(parentAuthorizerService)
        .append(", ")
        .append("parentVeniceZkBasePath:")
        .append(parentVeniceZkBasePath)
        .append(", ")
        .append("childVeniceZkBasePath:")
        .append(childVeniceZkBasePath)
        .toString();
  }

  private VeniceMultiRegionClusterCreateOptions(Builder builder) {
    numberOfRegions = builder.numberOfRegions;
    numberOfClusters = builder.numberOfClusters;
    numberOfParentControllers = builder.numberOfParentControllers;
    numberOfChildControllers = builder.numberOfChildControllers;
    numberOfServers = builder.numberOfServers;
    numberOfRouters = builder.numberOfRouters;
    replicationFactor = builder.replicationFactor;
    parentControllerProperties = builder.parentControllerProperties;
    childControllerProperties = builder.childControllerProperties;
    serverProperties = builder.serverProperties;
    sslToStorageNodes = builder.sslToStorageNodes;
    sslToKafka = builder.sslToKafka;
    forkServer = builder.forkServer;
    parentAuthorizerService = builder.parentAuthorizerService;
    parentVeniceZkBasePath = builder.parentVeniceZkBasePath;
    childVeniceZkBasePath = builder.childVeniceZkBasePath;
    accessController = builder.accessController;
  }

  public static class Builder {
    private int numberOfRegions;
    private int numberOfClusters;
    private int numberOfParentControllers = DEFAULT_NUMBER_OF_CONTROLLERS;
    private int numberOfChildControllers = DEFAULT_NUMBER_OF_CONTROLLERS;
    private int numberOfServers = DEFAULT_NUMBER_OF_SERVERS;
    private int numberOfRouters = DEFAULT_NUMBER_OF_ROUTERS;
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    private boolean sslToStorageNodes = DEFAULT_SSL_TO_STORAGE_NODES;
    private boolean sslToKafka = DEFAULT_SSL_TO_KAFKA;
    private boolean forkServer = false;
    private Properties parentControllerProperties;
    private Properties childControllerProperties;
    private Properties serverProperties;
    private AuthorizerService parentAuthorizerService;
    private String parentVeniceZkBasePath = "/";
    private String childVeniceZkBasePath = "/";
    private DynamicAccessController accessController;

    public Builder numberOfRegions(int numberOfRegions) {
      this.numberOfRegions = numberOfRegions;
      return this;
    }

    public Builder numberOfClusters(int numberOfClusters) {
      this.numberOfClusters = numberOfClusters;
      return this;
    }

    public Builder numberOfParentControllers(int numberOfParentControllers) {
      this.numberOfParentControllers = numberOfParentControllers;
      return this;
    }

    public Builder numberOfChildControllers(int numberOfChildControllers) {
      this.numberOfChildControllers = numberOfChildControllers;
      return this;
    }

    public Builder numberOfServers(int numberOfServers) {
      this.numberOfServers = numberOfServers;
      return this;
    }

    public Builder numberOfRouters(int numberOfRouters) {
      this.numberOfRouters = numberOfRouters;
      return this;
    }

    public Builder replicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder sslToStorageNodes(boolean sslToStorageNodes) {
      this.sslToStorageNodes = sslToStorageNodes;
      return this;
    }

    public Builder sslToKafka(boolean sslToKafka) {
      this.sslToKafka = sslToKafka;
      return this;
    }

    public Builder forkServer(boolean forkServer) {
      this.forkServer = forkServer;
      return this;
    }

    public Builder parentControllerProperties(Properties parentControllerProperties) {
      this.parentControllerProperties = parentControllerProperties;
      return this;
    }

    public Builder childControllerProperties(Properties childControllerProperties) {
      this.childControllerProperties = childControllerProperties;
      return this;
    }

    public Builder serverProperties(Properties serverProperties) {
      this.serverProperties = serverProperties;
      return this;
    }

    public Builder parentAuthorizerService(AuthorizerService parentAuthorizerService) {
      this.parentAuthorizerService = parentAuthorizerService;
      return this;
    }

    public Builder parentVeniceZkBasePath(String veniceZkBasePath) {
      if (veniceZkBasePath == null || !veniceZkBasePath.startsWith("/")) {
        throw new IllegalArgumentException("Venice Zk base path must start with /");
      }

      this.parentVeniceZkBasePath = veniceZkBasePath;
      return this;
    }

    public Builder childVeniceZkBasePath(String veniceZkBasePath) {
      if (veniceZkBasePath == null || !veniceZkBasePath.startsWith("/")) {
        throw new IllegalArgumentException("Venice Zk base path must start with /");
      }

      this.childVeniceZkBasePath = veniceZkBasePath;
      return this;
    }

    public Builder accessController(DynamicAccessController accessController) {
      this.accessController = accessController;
      return this;
    }

    private void addDefaults() {
      if (numberOfRegions == 0) {
        numberOfRegions = 1;
      }
      if (numberOfClusters == 0) {
        numberOfClusters = 1;
      }
    }

    public VeniceMultiRegionClusterCreateOptions build() {
      addDefaults();
      return new VeniceMultiRegionClusterCreateOptions(this);
    }
  }
}
