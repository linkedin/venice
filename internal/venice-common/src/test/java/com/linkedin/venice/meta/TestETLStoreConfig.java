package com.linkedin.venice.meta;

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

}
