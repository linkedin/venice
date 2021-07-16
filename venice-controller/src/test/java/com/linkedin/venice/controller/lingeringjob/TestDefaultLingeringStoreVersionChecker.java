package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestDefaultLingeringStoreVersionChecker {

  private DefaultLingeringStoreVersionChecker checker;
  private Store store;
  private Version version;
  private Time time;
  private Admin admin;

  @BeforeMethod
  public void setup() {
    checker = new DefaultLingeringStoreVersionChecker();
    store = mock(Store.class);
    version = mock(Version.class);
    time = mock(Time.class);
    admin = mock(Admin.class);
  }

  @Test
  public void testJobLingeringCase() {
    when(version.getCreatedTime()).thenReturn(1L);
    when(store.getBootstrapToOnlineTimeoutInHours()).thenReturn(1);
    when(time.getMilliseconds()).thenReturn(TimeUnit.HOURS.toMillis(1) + 2);
    Assert.assertTrue(checker.isStoreVersionLingering(store, version, time, admin, Optional.of("principal")));
  }

  @Test
  public void testJobNotLingeringCase() {
    when(version.getCreatedTime()).thenReturn(3L);
    when(store.getBootstrapToOnlineTimeoutInHours()).thenReturn(1);
    when(time.getMilliseconds()).thenReturn(TimeUnit.HOURS.toMillis(1) + 2);
    Assert.assertFalse(checker.isStoreVersionLingering(store, version, time, admin, Optional.of("principal")));
  }
}
