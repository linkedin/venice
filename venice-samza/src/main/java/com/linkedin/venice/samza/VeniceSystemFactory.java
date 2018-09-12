package com.linkedin.venice.samza;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;


public class VeniceSystemFactory implements SystemFactory {
  private static final Logger LOGGER = Logger.getLogger(VeniceSystemFactory.class);

  public static final String D2_ZK_HOSTS_PROPERTY = "__r2d2DefaultClient__.r2d2Client.zkHosts";

  public static final String SYSTEMS_PREFIX = "systems.";
  public static final String DOT = ".";
  public static final String DEPLOYMENT_ID = "deployment.id";

  public static final String VENICE_PUSH_TYPE = "push.type";

  /**
   * Venice store name Samza application is going to produce to.
   */
  public static final String VENICE_STORE = "store";

  /**
   * Whether to leverage Venice aggregation.
   * By default, it is 'false'.
   *
   * When the Samza application decides to leverage Venice aggregation, all the messages
   * will be produced to Venice Parent cluster, otherwise, all the messages will be produced
   * to local cluster.
   */
  public static final String VENICE_AGGREGATE = "aggregate";

  /**
   * D2 ZK hosts for Venice Parent Cluster.
   */
  public static final String VENICE_PARENT_D2_ZK_HOSTS = "venice.parent.d2.zk.hosts";

  // D2 service name for local cluster
  public static final String VENICE_LOCAL_D2_SERVICE = "VeniceController";
  // D2 service name for parent cluster
  public static final String VENICE_PARENT_D2_SERVICE = "VeniceParentController";

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("There is no Venice Consumer");
  }

  // Extra `Config` parameter is to ease the internal implementation
  protected SystemProducer createSystemProducer(String veniceD2ZKHost, String veniceD2Service, String storeName,
      ControllerApiConstants.PushType venicePushType, String samzaJobId, Config config) {
    return new VeniceSystemProducer(veniceD2ZKHost, veniceD2Service, storeName, venicePushType, samzaJobId);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    String samzaJobId = config.get(DEPLOYMENT_ID);

    String prefix = SYSTEMS_PREFIX + systemName + DOT;
    String pushTypeString = config.get(prefix + VENICE_PUSH_TYPE);
    ControllerApiConstants.PushType venicePushType;
    try {
      venicePushType = ControllerApiConstants.PushType.valueOf(pushTypeString);
    } catch (Exception e) {
      throw new SamzaException("Cannot parse venice push type: " + pushTypeString
      + ".  Must be one of: " + Arrays.stream(ControllerApiConstants.PushType.values())
          .map(Enum::toString)
          .collect(Collectors.joining(",")));
    }
    String storeName = config.get(prefix + VENICE_STORE);
    if (isEmpty(storeName)) {
      throw new SamzaException(VENICE_STORE + " should not be null for system " + systemName);
    }
    boolean veniceAggregate = config.getBoolean(prefix + VENICE_AGGREGATE, false);
    String veniceParentZKHosts = config.get(VENICE_PARENT_D2_ZK_HOSTS);
    if (isEmpty(veniceParentZKHosts)) {
      throw new SamzaException(VENICE_PARENT_D2_ZK_HOSTS + " should not be null, please put this property in your app-def.xml");
    }
    String localVeniceZKHosts = config.get(D2_ZK_HOSTS_PROPERTY);
    if (isEmpty(localVeniceZKHosts)) {
      throw new SamzaException(D2_ZK_HOSTS_PROPERTY + " should not be null");
    }

    LOGGER.info("Configs for " + systemName + " producer: ");
    LOGGER.info(prefix + VENICE_STORE + ": " + storeName);
    LOGGER.info(prefix + VENICE_AGGREGATE + ": " + veniceAggregate);
    LOGGER.info(prefix + VENICE_PUSH_TYPE + ": " + venicePushType);
    LOGGER.info(VENICE_PARENT_D2_ZK_HOSTS + ": " + veniceParentZKHosts);
    LOGGER.info(D2_ZK_HOSTS_PROPERTY + ": " + localVeniceZKHosts);

    String veniceD2ZKHost;
    String veniceD2Service;
    if (veniceAggregate) {
      veniceD2ZKHost = veniceParentZKHosts;
      veniceD2Service = VENICE_PARENT_D2_SERVICE;
    } else {
      veniceD2ZKHost = localVeniceZKHosts;
      veniceD2Service = VENICE_LOCAL_D2_SERVICE;
    }
    LOGGER.info("Will use the following Venice D2 ZK hosts: " + veniceD2ZKHost);
    return createSystemProducer(veniceD2ZKHost, veniceD2Service, storeName, venicePushType, samzaJobId, config);
  }

  private static boolean isEmpty(String input) {
    return (null == input) || input.isEmpty() || input.equals("null");
  }
}
