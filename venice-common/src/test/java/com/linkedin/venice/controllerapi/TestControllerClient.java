package com.linkedin.venice.controllerapi;

import com.linkedin.venice.utils.TestUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;


/**
 * Created by mwise on 6/1/16.
 */
public class TestControllerClient {

  @Test
  public static void clientReturnsErrorObjectOnConnectionFailure(){
    ControllerClient client = new ControllerClient(TestUtils.getUniqueString("cluster"), "http://localhost:17079");
    StoreResponse r3 = client.getStore("mystore");
    Assert.assertTrue(r3.isError());
  }

  private static class TestJsonObject {
    private String field1;
    private String field2;

    public String getField1() {
      return field1;
    }
    public String getField2() {
      return field2;
    }
    public void setField1(String fld) {
      field1 = fld;
    }
    public void setField2(String fld) {
      field2 = fld;
    }
  }
  @Test
  public void testObjectMapperIgnoringUnknownProperties() throws IOException {
    ObjectMapper objectMapper = ControllerClient.getObjectMapper();
    String field1Value = "field1_value";
    String jsonStr = "{\"field1\":\"" + field1Value + "\",\"field3\":\"" + field1Value + "\"}";
    TestJsonObject jsonObject = objectMapper.readValue(jsonStr, TestJsonObject.class);
    Assert.assertEquals(jsonObject.getField1(), field1Value);
    Assert.assertNull(jsonObject.getField2());
  }
}