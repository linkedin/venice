package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.Store;
import java.util.Base64;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePathParser {


  VeniceVersionFinder getVersionFinder(){
    //Mock objects
    Store mockStore = Mockito.mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    MetadataRepository mockMetadataRepository = Mockito.mock(MetadataRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

    return new VeniceVersionFinder(mockMetadataRepository);
  }

  @Test
  public void parsesQueries() throws RouterException {
    String uri = "read/store/key";
    VenicePathParser parser = new VenicePathParser(getVersionFinder());
    Path path = parser.parseResourceUri(uri);
    String keyb64 = Base64.getEncoder().encodeToString("key".getBytes());
    Assert.assertEquals(path.getLocation(), "read/store_v1/" + keyb64 + "?" + VenicePathParser.B64FORMAT);

    Path path2 = parser.substitutePartitionKey(path, RouterKey.fromString("key2"));
    String key2b64 = Base64.getEncoder().encodeToString("key2".getBytes());
    Assert.assertEquals(path2.getLocation(), "read/store_v1/" + key2b64 + "?" + VenicePathParser.B64FORMAT);
  }

  @Test
  public void parsesB64Uri() throws RouterException {
    String myUri = "/read/storename/bXlLZXk=?f=b64";
    String expectedKey = "myKey";
    Path path = new VenicePathParser(getVersionFinder()).parseResourceUri(myUri);
    Assert.assertEquals(path.getPartitionKey().getBytes(), expectedKey.getBytes(),
        new String(path.getPartitionKey().getBytes()) + " should match " + expectedKey);
  }

  @Test(expectedExceptions = RouterException.class)
  public void failsToParseOtherActions() throws RouterException {
    new VenicePathParser(getVersionFinder()).parseResourceUri("/badaction/storename/key");
  }

  @Test
  public void validatesResourceNames() {
    String[] goodNames = {
        "goodName",
        "good_name_with_underscores",
        "good-name-with-dashes",
        "goodNameWithNumbers1234545"
    };

    for (String name : goodNames){
      Assert.assertTrue(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should be valid");
    }

    String[] badNames = {
        "bad name with space",
        "bad.name.with.dots",
        "8startswithnumber",
        "bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars-bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars"
    };

    for (String name : badNames){
      Assert.assertFalse(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should not be valid");
    }

  }
}
