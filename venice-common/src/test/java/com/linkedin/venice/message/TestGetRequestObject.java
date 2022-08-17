package com.linkedin.venice.message;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 1/14/16.
 */
public class TestGetRequestObject {
  @Test
  public void getRequestCanBeSerializedAndDeserialized() {
    char[] topic = "mytopic".toCharArray();
    byte[] key = "mykey".getBytes();
    int partition = 17;

    GetRequestObject newRequest = new GetRequestObject(topic, partition, key);

    Assert.assertEquals(topic, newRequest.getTopic());
    Assert.assertEquals(key, newRequest.getKey());
    Assert.assertEquals(partition, newRequest.getPartition());

    byte[] serializedRequest = newRequest.serialize();

    Assert.assertTrue(GetRequestObject.isPayloadComplete(serializedRequest));

    GetRequestObject deserializedRequest = GetRequestObject.deserialize(serializedRequest);

    Assert.assertEquals(topic, deserializedRequest.getTopic());
    Assert.assertEquals(key, deserializedRequest.getKey());
    Assert.assertEquals(partition, deserializedRequest.getPartition());
  }

  @Test
  public void getRequestObjectKnowsItIsIncomplete() {
    byte oneByte = 1;
    byte[] singleByteArray = new byte[1];
    singleByteArray[0] = oneByte;

    Assert.assertFalse(GetRequestObject.isPayloadComplete(singleByteArray));

    char[] topic = "mytopic".toCharArray();
    byte[] key = "mykey".getBytes();
    int partition = 17;

    GetRequestObject newRequest = new GetRequestObject(topic, partition, key);
    byte[] serializedRequest = newRequest.serialize();
    byte[] header = new byte[GetRequestObject.HEADER_SIZE];
    ByteBuffer.wrap(serializedRequest).get(header);

    Assert.assertTrue(GetRequestObject.isHeaderComplete(header));
    Assert.assertFalse(GetRequestObject.isPayloadComplete(header));

    byte[] incomplete = new byte[serializedRequest.length - 1];
    ByteBuffer.wrap(serializedRequest).get(incomplete);
    Assert.assertTrue(GetRequestObject.isHeaderComplete(incomplete));
    Assert.assertFalse(GetRequestObject.isPayloadComplete(incomplete));

  }
}
