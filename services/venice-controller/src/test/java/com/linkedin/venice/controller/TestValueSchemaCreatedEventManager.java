package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.ValueSchemaCreatedListener;
import com.linkedin.venice.schema.SchemaEntry;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class TestValueSchemaCreatedEventManager {
  private static final String STORE_NAME = "testStore";

  @Test
  public void testListenersInvokedInRegistrationOrder() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener listener1 = mock(ValueSchemaCreatedListener.class);
    ValueSchemaCreatedListener listener2 = mock(ValueSchemaCreatedListener.class);
    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");

    manager.addListener(listener1);
    manager.addListener(listener2);

    manager.notifyValueSchemaCreated(STORE_NAME, schemaEntry, true);

    InOrder inOrder = inOrder(listener1, listener2);
    inOrder.verify(listener1).handleValueSchemaCreated(eq(STORE_NAME), eq(schemaEntry), eq(true));
    inOrder.verify(listener2).handleValueSchemaCreated(eq(STORE_NAME), eq(schemaEntry), eq(true));
  }

  @Test
  public void testListenerExceptionDoesNotPreventOtherListeners() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener throwing = mock(ValueSchemaCreatedListener.class);
    doThrow(new RuntimeException("boom")).when(throwing).handleValueSchemaCreated(any(), any(), anyBoolean());
    ValueSchemaCreatedListener second = mock(ValueSchemaCreatedListener.class);

    manager.addListener(throwing);
    manager.addListener(second);

    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");
    manager.notifyValueSchemaCreated(STORE_NAME, schemaEntry, false);

    // Both listeners get the call; the throw is caught and logged.
    verify(throwing, times(1)).handleValueSchemaCreated(eq(STORE_NAME), eq(schemaEntry), eq(false));
    verify(second, times(1)).handleValueSchemaCreated(eq(STORE_NAME), eq(schemaEntry), eq(false));
  }
}
