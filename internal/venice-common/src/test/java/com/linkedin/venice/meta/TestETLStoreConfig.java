package com.linkedin.venice.meta;

import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestETLStoreConfig {
  @Test
  public void getEtlActiveFabricsReturnsNullWhenUnset() {
    ETLStoreConfig config = new ETLStoreConfigImpl();
    Assert.assertNull(config.getEtlActiveFabrics());
  }

  @Test
  public void getEtlActiveFabricsReturnsCopyWhenSet() {
    List<String> fabrics = Arrays.asList("dc-0", "dc-1");
    ETLStoreConfig config = new ETLStoreConfigImpl("proxy", true, true, 2, fabrics);
    Assert.assertEquals(config.getEtlActiveFabrics(), fabrics);
  }

  @Test
  public void setEtlActiveFabricsAcceptsNull() {
    ETLStoreConfig config = new ETLStoreConfigImpl("proxy", true, true, 2, Arrays.asList("dc-0"));
    config.setEtlActiveFabrics(null);
    Assert.assertNull(config.getEtlActiveFabrics());
  }

  @Test
  public void setEtlActiveFabricsAcceptsList() {
    ETLStoreConfig config = new ETLStoreConfigImpl();
    config.setEtlActiveFabrics(Arrays.asList("dc-0", "dc-1"));
    Assert.assertEquals(config.getEtlActiveFabrics(), Arrays.asList("dc-0", "dc-1"));
  }

  @Test
  public void setEtlActiveFabricsAcceptsEmptyList() {
    ETLStoreConfig config = new ETLStoreConfigImpl();
    config.setEtlActiveFabrics(Collections.emptyList());
    Assert.assertEquals(config.getEtlActiveFabrics(), Collections.emptyList());
  }

  @Test
  public void cloneIncludesEtlActiveFabrics() {
    ETLStoreConfig original = new ETLStoreConfigImpl("proxy", true, false, 2, Arrays.asList("dc-0"));
    ETLStoreConfig clone = original.clone();
    Assert.assertEquals(clone.getEtlActiveFabrics(), Arrays.asList("dc-0"));
    Assert.assertEquals(clone, original);
  }

  @Test
  public void initialOnboardingValuesDefaultToNull() {
    ETLStoreConfig config = new ETLStoreConfigImpl("proxy", true, true, 2, null);
    Assert.assertNull(config.getInitialSlaTier());
    Assert.assertNull(config.getInitialGrantAclUsers());
    Assert.assertNull(config.getInitialWorkloadType());
    Assert.assertNull(new ETLStoreConfigImpl().getInitialSlaTier());
  }

  @Test
  public void initialOnboardingValuesCanBeSetAndCleared() {
    ETLStoreConfig config = new ETLStoreConfigImpl();
    config.setInitialSlaTier("TIER_A");
    config.setInitialGrantAclUsers(Arrays.asList("user-a", "user-b"));
    config.setInitialWorkloadType("BATCH");
    Assert.assertEquals(config.getInitialSlaTier(), "TIER_A");
    Assert.assertEquals(config.getInitialGrantAclUsers(), Arrays.asList("user-a", "user-b"));
    Assert.assertEquals(config.getInitialWorkloadType(), "BATCH");
    Assert.assertEquals(config.dataModel().initialSlaTier.toString(), "TIER_A");

    config.setInitialSlaTier(null);
    config.setInitialGrantAclUsers(null);
    config.setInitialWorkloadType(null);
    Assert.assertNull(config.getInitialSlaTier());
    Assert.assertNull(config.getInitialGrantAclUsers());
    Assert.assertNull(config.getInitialWorkloadType());
  }

  @Test
  public void cloneAndEqualsIncludeInitialOnboardingValues() {
    ETLStoreConfig original = new ETLStoreConfigImpl("proxy", true, false, 2, Arrays.asList("dc-0"));
    original.setInitialSlaTier("TIER_A");
    original.setInitialGrantAclUsers(Arrays.asList("user-a"));
    original.setInitialWorkloadType("BATCH");

    ETLStoreConfig clone = original.clone();
    Assert.assertEquals(clone, original);
    Assert.assertEquals(clone.getInitialSlaTier(), "TIER_A");
    Assert.assertEquals(clone.getInitialGrantAclUsers(), Arrays.asList("user-a"));
    Assert.assertEquals(clone.getInitialWorkloadType(), "BATCH");

    clone.setInitialSlaTier("TIER_B");
    Assert.assertNotEquals(clone, original);
    Assert.assertEquals(original.getInitialSlaTier(), "TIER_A", "Changing the clone must not change the original");
  }

  @Test
  public void initialOnboardingValuesSurviveJsonRoundTrip() throws Exception {
    ETLStoreConfig config = new ETLStoreConfigImpl("proxy", true, false, 2, Arrays.asList("dc-0"));
    config.setInitialSlaTier("TIER_A");
    config.setInitialGrantAclUsers(Arrays.asList("user-a", "user-b"));
    config.setInitialWorkloadType("BATCH");

    String json = ObjectMapperFactory.getInstance().writeValueAsString(config);
    ETLStoreConfig roundTripped = ObjectMapperFactory.getInstance().readValue(json, ETLStoreConfig.class);
    Assert.assertEquals(roundTripped, config);
    Assert.assertEquals(roundTripped.getInitialGrantAclUsers(), Arrays.asList("user-a", "user-b"));

    // JSON written before these fields existed still deserializes, with the values unset.
    ETLStoreConfig legacy = ObjectMapperFactory.getInstance()
        .readValue(
            "{\"etledUserProxyAccount\":\"proxy\",\"regularVersionETLEnabled\":true,"
                + "\"futureVersionETLEnabled\":false}",
            ETLStoreConfig.class);
    Assert.assertEquals(legacy.getEtledUserProxyAccount(), "proxy");
    Assert.assertNull(legacy.getInitialSlaTier());
    Assert.assertNull(legacy.getInitialGrantAclUsers());
    Assert.assertNull(legacy.getInitialWorkloadType());
  }
}
