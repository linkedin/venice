package com.linkedin.venice.meta;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 5/9/16.
 */
public class TestVersion {
  @Test
  public void identifiesValidTopicNames(){
    String goodTopic = "my_very_good_store_v4";
    Assert.assertTrue(Version.topicIsValidStoreVersion(goodTopic),
        goodTopic + " should parse as a valid store-version topic");

    String badTopic = "__consumer_offsets";
    Assert.assertFalse(Version.topicIsValidStoreVersion(badTopic),
        badTopic + " must not parse as a valid store-version topic");
  }

}
