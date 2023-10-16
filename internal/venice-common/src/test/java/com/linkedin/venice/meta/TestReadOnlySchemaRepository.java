package com.linkedin.venice.meta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestReadOnlySchemaRepository {
  @Test
  public void testGetLatestDerivedSchema() {
    String storeName = "test_store";
    // Latest value schema has a derived schema
    SchemaEntry latestValueSchema = new SchemaEntry(1, "\"string\"");
    ReadOnlySchemaRepository repository1 = mock(ReadOnlySchemaRepository.class);
    when(repository1.getSupersetOrLatestValueSchema(storeName)).thenReturn(latestValueSchema);
    DerivedSchemaEntry derivedSchemaEntry = new DerivedSchemaEntry(1, 1, "\"string\"");
    when(repository1.getLatestDerivedSchema(storeName, 1)).thenReturn(derivedSchemaEntry);
    when(repository1.getLatestDerivedSchema(storeName)).thenCallRealMethod();

    Assert.assertEquals(repository1.getLatestDerivedSchema(storeName), derivedSchemaEntry);

    // Latest value schema doesn't have a corresponding derived schema.
    SchemaEntry latestSupersetSchema = new SchemaEntry(10, "\"string\"");
    ReadOnlySchemaRepository repository2 = mock(ReadOnlySchemaRepository.class);
    when(repository2.getSupersetOrLatestValueSchema(storeName)).thenReturn(latestSupersetSchema);
    when(repository2.getLatestDerivedSchema(storeName, 10)).thenThrow(new VeniceException("No derived schema"));
    List<DerivedSchemaEntry> derivedSchemaEntryList = new ArrayList<>();
    DerivedSchemaEntry latestDerivedSchema = new DerivedSchemaEntry(9, 2, "\"string\"");
    derivedSchemaEntryList.add(new DerivedSchemaEntry(8, 1, "\"string\""));
    derivedSchemaEntryList.add(new DerivedSchemaEntry(8, 2, "\"string\""));
    derivedSchemaEntryList.add(latestDerivedSchema);
    derivedSchemaEntryList.add(new DerivedSchemaEntry(9, 1, "\"string\""));
    when(repository2.getDerivedSchemas(storeName)).thenReturn(derivedSchemaEntryList);

    when(repository2.getLatestDerivedSchema(storeName)).thenCallRealMethod();

    Assert.assertEquals(repository2.getLatestDerivedSchema(storeName), latestDerivedSchema);
  }

}
