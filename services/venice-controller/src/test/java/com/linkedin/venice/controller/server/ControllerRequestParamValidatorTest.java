package com.linkedin.venice.controller.server;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ControllerRequestParamValidatorTest {
  @Test(dataProvider = "validInputProvider")
  public void testCreateStoreRequestValidatorValidInputs(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema) {
    ControllerRequestParamValidator.createStoreRequestValidator(clusterName, storeName, owner, keySchema, valueSchema);
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
  public Object[][] validInputProvider() {
    return new Object[][] { { "clusterA", "storeA", "ownerA", "keySchemaA", "valueSchemaA" },
        { "clusterB", "storeB", "ownerB", "keySchemaB", "valueSchemaB" } };
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
}
