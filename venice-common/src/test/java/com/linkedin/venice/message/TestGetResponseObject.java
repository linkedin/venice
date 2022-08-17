package com.linkedin.venice.message;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 1/14/16.
 */
public class TestGetResponseObject {
  @Test
  public void getResponseCanBeSerializedAndDeserialized() {

    byte[] value = "matt wise is awesome".getBytes();
    GetResponseObject r = new GetResponseObject(value);
    Assert.assertEquals(value, r.getValue());

    byte[] serializedResponse = r.serialize();
    Assert.assertTrue(GetResponseObject.isPayloadComplete(serializedResponse));

    GetResponseObject deserializedResponse = GetResponseObject.deserialize(serializedResponse);
    Assert.assertEquals(value, deserializedResponse.getValue());
  }

  @Test
  public void getResponseObjectKnowsItIsIncomplete() {
    byte oneByte = 1;
    byte[] singleByteArray = new byte[1];
    singleByteArray[0] = oneByte;

    Assert.assertFalse(GetResponseObject.isPayloadComplete(singleByteArray));

    byte[] value = "matt wise is awesome".getBytes();
    GetResponseObject r = new GetResponseObject(value);

    byte[] serialized = r.serialize();
    byte[] header = new byte[GetResponseObject.HEADER_SIZE];
    ByteBuffer.wrap(serialized).get(header);

    Assert.assertTrue(GetResponseObject.isHeaderComplete(header));
    Assert.assertFalse(GetResponseObject.isPayloadComplete(header));

  }
}
