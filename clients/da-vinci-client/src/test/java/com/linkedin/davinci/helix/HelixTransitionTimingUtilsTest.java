package com.linkedin.davinci.helix;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.testng.annotations.Test;


public class HelixTransitionTimingUtilsTest {
  @Test
  public void testFormatTransitionTiming() {
    // Case 1: Null message
    String result = HelixTransitionTimingUtils.formatTransitionTiming(null, null);
    assertEquals(result, "[deltas: unavailable - null message]");

    // Case 2: Valid timestamps with proper deltas
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    HelixProperty.Stat stat = mock(HelixProperty.Stat.class);
    long baseTime = System.currentTimeMillis() - 10000;
    when(message.getCreateTimeStamp()).thenReturn(baseTime);
    when(message.getExecuteStartTimeStamp()).thenReturn(baseTime + 5000);
    when(message.getReadTimeStamp()).thenReturn(baseTime + 2000);
    when(message.getStat()).thenReturn(stat);
    when(stat.getCreationTime()).thenReturn(baseTime + 100);
    when(context.getCreationTime()).thenReturn(baseTime + 3000);

    result = HelixTransitionTimingUtils.formatTransitionTiming(message, context);
    assertNotNull(result);
    assertTrue(result.contains("controllerToZkDelayMs=100"));
    assertTrue(result.contains("createToExecuteMs=5000"));
    assertTrue(result.contains("queueDelayMs=3000"));

    // Case 3: Missing timestamps (null stat and context)
    message = mock(Message.class);
    when(message.getCreateTimeStamp()).thenReturn(baseTime);
    when(message.getExecuteStartTimeStamp()).thenReturn(baseTime + 1000);
    when(message.getReadTimeStamp()).thenReturn(baseTime + 500);
    when(message.getStat()).thenReturn(null);

    result = HelixTransitionTimingUtils.formatTransitionTiming(message, null);
    assertNotNull(result);
    assertTrue(result.contains("contextLagMs=-1"));
    assertTrue(result.contains("controllerToZkDelayMs=-1"));
    assertTrue(result.contains("createToExecuteMs=1000"));

    // Case 4: Exception handling
    message = mock(Message.class);
    when(message.getCreateTimeStamp()).thenThrow(new RuntimeException("Test exception"));
    result = HelixTransitionTimingUtils.formatTransitionTiming(message, null);
    assertTrue(result.contains("[deltas: unavailable due to error:"));
    assertTrue(result.contains("Test exception"));
  }

  @Test
  public void testFormatNotificationContext() {
    // Case 1: Null context
    String result = HelixTransitionTimingUtils.formatNotificationContext(null);
    assertEquals(result, "null");

    // Case 2: Full context with all fields
    NotificationContext context = mock(NotificationContext.class);
    HelixManager manager = mock(HelixManager.class);
    when(context.getEventName()).thenReturn("CurrentStateChange");
    when(context.getType()).thenReturn(NotificationContext.Type.CALLBACK);
    when(context.getPathChanged()).thenReturn("/test/path");
    when(context.getCreationTime()).thenReturn(System.currentTimeMillis() - 1000);
    when(context.getIsChildChange()).thenReturn(true);
    when(context.getManager()).thenReturn(manager);
    when(manager.getInstanceName()).thenReturn("instance1");
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getSessionId()).thenReturn("session123");

    result = HelixTransitionTimingUtils.formatNotificationContext(context);
    assertNotNull(result);
    assertTrue(result.contains("eventName=CurrentStateChange"));
    assertTrue(result.contains("type=CALLBACK"));
    assertTrue(result.contains("instanceName=instance1"));
    assertTrue(result.contains("isChildChange=true"));

    // Case 3: Minimal context (only required fields)
    context = mock(NotificationContext.class);
    when(context.getEventName()).thenReturn(null);
    when(context.getType()).thenReturn(null);
    when(context.getPathChanged()).thenReturn(null);
    when(context.getCreationTime()).thenReturn(System.currentTimeMillis());
    when(context.getIsChildChange()).thenReturn(false);
    when(context.getManager()).thenReturn(null);

    result = HelixTransitionTimingUtils.formatNotificationContext(context);
    assertNotNull(result);
    assertTrue(result.contains("creationTime="));
    assertTrue(result.contains("ageMs="));
    assertTrue(result.startsWith("{"));
    assertTrue(result.endsWith("}"));
  }
}
