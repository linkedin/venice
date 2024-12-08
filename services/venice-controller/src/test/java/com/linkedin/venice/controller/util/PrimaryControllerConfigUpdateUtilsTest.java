package com.linkedin.venice.controller.util;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class PrimaryControllerConfigUpdateUtilsTest {
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String SECOND_VALUE_FIELD_NAME = "opt_int_field";
  private static final String VALUE_SCHEMA_V1_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\", \"default\": 10}]\n" + "}";
  private static final String VALUE_SCHEMA_V2_STR =
      "{\n" + "\"type\": \"record\",\n" + "\"name\": \"TestValueSchema\",\n"
          + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n" + "\"fields\": [\n" + "{\"name\": \""
          + SECOND_VALUE_FIELD_NAME + "\", \"type\": [\"null\", \"int\"], \"default\": null}]\n" + "}";
  private static final String SUPERSET_VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\", \"default\": 10},\n"
      + "{\"name\": \"" + SECOND_VALUE_FIELD_NAME + "\", \"type\": [\"null\", \"int\"], \"default\": null}]\n" + "}";

  @Test
  public void testRegisterInferredSchemas() {
    String clusterName = "clusterName";
    String storeName = "storeName";
    Collection<SchemaEntry> storeValueSchemas =
        Arrays.asList(new SchemaEntry(1, VALUE_SCHEMA_V1_STR), new SchemaEntry(2, VALUE_SCHEMA_V2_STR));
    SchemaEntry supersetSchemaEntry = new SchemaEntry(3, SUPERSET_VALUE_SCHEMA_STR);

    Admin mockAdmin = mock(Admin.class);
    Store store = mock(Store.class);

    reset(mockAdmin);
    reset(store);
    setupMocks(mockAdmin, store, clusterName, storeName, storeValueSchemas, supersetSchemaEntry);

    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    doReturn(true).when(store).isReadComputationEnabled();
    PrimaryControllerConfigUpdateUtils.registerInferredSchemas(mockAdmin, clusterName, storeName);
    validateSuperSetSchemaGenerated(mockAdmin, clusterName, storeName);

    reset(mockAdmin);
    reset(store);
    setupMocks(mockAdmin, store, clusterName, storeName, storeValueSchemas, supersetSchemaEntry);

    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    doReturn(true).when(store).isWriteComputationEnabled();
    PrimaryControllerConfigUpdateUtils.registerInferredSchemas(mockAdmin, clusterName, storeName);
    validateSuperSetSchemaGenerated(mockAdmin, clusterName, storeName);
    validateUpdateSchemaGenerated(mockAdmin, clusterName, storeName);

    reset(mockAdmin);
    reset(store);
    setupMocks(mockAdmin, store, clusterName, storeName, storeValueSchemas, supersetSchemaEntry);

    doReturn(1).when(store).getLatestSuperSetValueSchemaId();
    PrimaryControllerConfigUpdateUtils.registerInferredSchemas(mockAdmin, clusterName, storeName);
    validateSuperSetSchemaGenerated(mockAdmin, clusterName, storeName);

    reset(mockAdmin);
    reset(store);
    setupMocks(mockAdmin, store, clusterName, storeName, storeValueSchemas, supersetSchemaEntry);

    doReturn(true).when(store).isActiveActiveReplicationEnabled();
    doReturn(1).when(store).getRmdVersion();
    PrimaryControllerConfigUpdateUtils.registerInferredSchemas(mockAdmin, clusterName, storeName);
    validateRmdSchemaGenerated(mockAdmin, clusterName, storeName);
  }

  private void setupMocks(
      Admin mockAdmin,
      Store store,
      String clusterName,
      String storeName,
      Collection<SchemaEntry> storeValueSchemas,
      SchemaEntry supersetSchemaEntry) {
    doReturn(storeName).when(store).getName();

    SupersetSchemaGenerator supersetSchemaGenerator = mock(SupersetSchemaGenerator.class);

    doReturn(true).when(mockAdmin).isPrimary();
    doReturn(false).when(mockAdmin).isParent();
    doReturn(store).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(supersetSchemaGenerator).when(mockAdmin).getSupersetSchemaGenerator(clusterName);
    doReturn(storeValueSchemas).when(mockAdmin).getValueSchemas(clusterName, storeName);

    doReturn(supersetSchemaEntry).when(supersetSchemaGenerator).generateSupersetSchemaFromSchemas(storeValueSchemas);

    HelixVeniceClusterResources clusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(clusterResources).when(mockAdmin).getHelixVeniceClusterResources(clusterName);

    ReadWriteSchemaRepository schemaRepository = mock(ReadWriteSchemaRepository.class);
    doReturn(schemaRepository).when(clusterResources).getSchemaRepository();

    doReturn(Collections.emptyList()).when(schemaRepository).getReplicationMetadataSchemas(storeName);
  }

  private void validateSuperSetSchemaGenerated(Admin mockAdmin, String clusterName, String storeName) {
    verify(mockAdmin).addSupersetSchema(
        clusterName,
        storeName,
        null,
        SchemaData.INVALID_VALUE_SCHEMA_ID,
        SUPERSET_VALUE_SCHEMA_STR,
        3);
  }

  private void validateUpdateSchemaGenerated(Admin mockAdmin, String clusterName, String storeName) {
    SchemaEntry updateSchemaEntry1 = new SchemaEntry(1, VALUE_SCHEMA_V1_STR);
    Schema updateSchema1 =
        WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(updateSchemaEntry1.getSchema());

    SchemaEntry updateSchemaEntry2 = new SchemaEntry(1, VALUE_SCHEMA_V2_STR);
    Schema updateSchema2 =
        WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(updateSchemaEntry2.getSchema());

    // Ideally, we should have seen the superset schema also, but due to the static-ness of mocks, we don't see it now
    verify(mockAdmin).addDerivedSchema(clusterName, storeName, 1, updateSchema1.toString());
    verify(mockAdmin).addDerivedSchema(clusterName, storeName, 2, updateSchema2.toString());
    verify(mockAdmin, times(2)).addDerivedSchema(eq(clusterName), eq(storeName), anyInt(), anyString());
  }

  private void validateRmdSchemaGenerated(Admin mockAdmin, String clusterName, String storeName) {
    Schema rmdSchema1 = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA_V1_STR, 1);
    Schema rmdSchema2 = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA_V2_STR, 1);

    // Ideally, we should have seen the superset schema also, but due to the static-ness of mocks, we don't see it now
    verify(mockAdmin).addReplicationMetadataSchema(clusterName, storeName, 1, 1, rmdSchema1.toString());
    verify(mockAdmin).addReplicationMetadataSchema(clusterName, storeName, 2, 1, rmdSchema2.toString());
    verify(mockAdmin, times(2))
        .addReplicationMetadataSchema(eq(clusterName), eq(storeName), anyInt(), eq(1), anyString());
  }
}
