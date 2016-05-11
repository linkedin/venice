package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.ReadonlyStoreRepository;
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
    ReadonlyStoreRepository mockRepo = Mockito.mock(ReadonlyStoreRepository.class);
    doReturn(null).when(mockRepo).getStore(anyString());
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo);
    try{
      versionFinder.getVersion("");
      Assert.fail("versionFinder.getVersion() on previous line should throw a RouterException");
    } catch (RouterException e) {
      Assert.assertEquals(e.getCode(), 400);
    }
  }
}
