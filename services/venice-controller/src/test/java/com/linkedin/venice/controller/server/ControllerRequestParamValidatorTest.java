package com.linkedin.venice.controller.server;

import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ControllerRequestParamValidatorTest {
  @Test
  public void testCreateStoreRequestValidatorValidInputs() {
    ControllerRequestParamValidator
        .createStoreRequestValidator("clusterA", "storeA", "ownerA", "keySchemaA", "valueSchemaA");
  }

  @Test(dataProvider = "missingParameterProvider", expectedExceptions = IllegalArgumentException.class)
  public void testCreateStoreRequestValidatorMissingParameters(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema) {
    ControllerRequestParamValidator.createStoreRequestValidator(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @DataProvider
  public Object[][] missingParameterProvider() {
    return new Object[][] { { null, "storeA", "ownerA", "keySchemaA", "valueSchemaA" }, // Missing clusterName
        { "", "storeA", "ownerA", "keySchemaA", "valueSchemaA" }, // Empty clusterName
        { "clusterA", null, "ownerA", "keySchemaA", "valueSchemaA" }, // Missing storeName
        { "clusterA", "", "ownerA", "keySchemaA", "valueSchemaA" }, // Empty storeName
        { "clusterA", "storeA", null, "keySchemaA", "valueSchemaA" }, // Missing owner
        { "clusterA", "storeA", "ownerA", null, "valueSchemaA" }, // Missing keySchema
        { "clusterA", "storeA", "ownerA", "", "valueSchemaA" }, // Empty keySchema
        { "clusterA", "storeA", "ownerA", "keySchemaA", null }, // Missing valueSchema
        { "clusterA", "storeA", "ownerA", "keySchemaA", "" } // Empty valueSchema
    };
  }

  @Test
  public void testValidateClusterStoreInfoValidInputs() {
    ControllerRequestParamValidator.validateClusterStoreInfo(
        ClusterStoreGrpcInfo.newBuilder().setClusterName("clusterA").setStoreName("storeA").build());
  }

  @Test(dataProvider = "missingClusterStoreInfoProvider", expectedExceptions = IllegalArgumentException.class)
  public void testValidateClusterStoreInfoMissingParameters(String clusterName, String storeName) {
    ClusterStoreGrpcInfo.Builder builder = ClusterStoreGrpcInfo.newBuilder();
    if (clusterName != null) {
      builder.setClusterName(clusterName);
    }
    if (storeName != null) {
      builder.setStoreName(storeName);
    }
    ControllerRequestParamValidator.validateClusterStoreInfo(builder.build());
  }

  @DataProvider
  public Object[][] missingClusterStoreInfoProvider() {
    return new Object[][] { { null, "storeA" }, // Missing clusterName
        { "", "storeA" }, // Empty clusterName
        { "clusterA", null }, // Missing storeName
        { "clusterA", "" } // Empty storeName
    };
  }
}
