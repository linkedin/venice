package com.linkedin.venice.samza;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;


public class VeniceSystemFactory implements SystemFactory {
  protected static final String SYSTEMS_PREFIX = "systems.";
  protected static final String DOT = ".";
  protected static final String JOB_ID = "job.id";
  /**
   * Http Venice URL (router or controller).  Used by producer to query controller.
   * TODO: integrate sdwu's work so we can also use D2 here.
   */
  protected static final String VENICE_URL = "url";
  protected static final String VENICE_CLUSTER = "cluster";
  protected static final String VENICE_PUSH_TYPE = "push.type";

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("There is no Venice Consumer");
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    String samzaJobId = config.get(JOB_ID);

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
    String veniceUrl = config.get(prefix + VENICE_URL);
    String veniceCluster = config.get(prefix + VENICE_CLUSTER);

    if (isEmpty(veniceCluster)) {
      throw new SamzaException(VENICE_CLUSTER + " must not be null for system " + systemName);
    }
    if (isEmpty(veniceUrl)) {
      throw new SamzaException(VENICE_URL + " must not be null for system " + systemName);
    }
    return new VeniceSystemProducer(veniceUrl, veniceCluster, venicePushType, samzaJobId);
  }

  private static boolean isEmpty(String input) {
    return (null == input) || input.isEmpty() || input.equals("null");
  }
}
