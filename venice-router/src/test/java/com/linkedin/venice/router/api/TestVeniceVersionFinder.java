package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.impl.Router;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/4/16.
 */
public class TestVeniceVersionFinder {
  @Test
  public void throws404onMissingStore(){
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    doReturn(null).when(mockRepo).getStore(anyString());
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo);
    try{
      versionFinder.getVersion("");
      Assert.fail("versionFinder.getVersion() on previous line should throw a RouterException");
    } catch (RouterException e) {
      Assert.assertEquals(e.code(), 400);
    }
  }

  @Test
  public void returnNonExistingVersionOnceStoreIsDisabled()
      throws RouterException {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    String storeName = "TestVeniceVersionFinder";
    int currentVersion = 10;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setCurrentVersion(currentVersion);
    // disable store, should return the number indicates that none of version is avaiable to read.
    store.setEnableReads(false);
    doReturn(store).when(mockRepo).getStore(storeName);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo);
    try {
      versionFinder.getVersion(storeName);
      Assert.fail("Store should be disabled and forbidden to read.");
    } catch (RouterException e) {
      Assert.assertEquals(e.status(), HttpResponseStatus.FORBIDDEN, "Store should be disabled and forbidden to read.");
    }
    // enable store, should return the correct current version.
    store.setEnableReads(true);
    Assert.assertEquals(versionFinder.getVersion(storeName), currentVersion);
  }
}
