package com.linkedin.venice.d2;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.d2.D2LoadBalancerStrategyProperties;
import com.linkedin.d2.D2QuarantineProperties;
import com.linkedin.d2.HttpMethod;
import com.linkedin.d2.balancer.config.LoadBalancerStrategyPropertiesConverter;
import com.linkedin.d2.balancer.config.RelativeStrategyPropertiesConverter;
import com.linkedin.d2.balancer.properties.PropertyKeys;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class D2ConfigUtilsTest {
  @DataProvider
  public Object[][] healthCheckEndpoints() {
    return new Object[][] { { false }, { true } };
  }

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "healthCheckEndpoints")
  public void testQuarantineDefaults(boolean useHealthCheckEndpoint) {
    Map<String, Object> defaults = D2ConfigUtils.getD2QuarantineDefaults(useHealthCheckEndpoint);
    D2LoadBalancerStrategyProperties legacy = LoadBalancerStrategyPropertiesConverter
        .toConfig((Map<String, Object>) defaults.get(PropertyKeys.LB_STRATEGY_PROPERTIES));
    D2QuarantineProperties relative = RelativeStrategyPropertiesConverter
        .toProperties((Map<String, Object>) defaults.get(PropertyKeys.RELATIVE_STRATEGY_PROPERTIES))
        .getQuarantineProperties();

    assertEquals(legacy.getQuarantineCfg().getQuarantineMaxPercent(), Double.valueOf(0.1));
    assertEquals(relative.getQuarantineMaxPercent(), Double.valueOf(0.1));
    if (useHealthCheckEndpoint) {
      assertEquals(legacy.getQuarantineCfg().getQuarantineMethod(), "GET:/health");
      assertEquals(relative.getHealthCheckMethod(), HttpMethod.GET);
      assertEquals(relative.getHealthCheckPath(), "/health");
    } else {
      assertFalse(legacy.getQuarantineCfg().hasQuarantineMethod());
      assertFalse(relative.hasHealthCheckMethod());
      assertFalse(relative.hasHealthCheckPath());
    }
  }
}
