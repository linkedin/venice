package com.linkedin.venice.listener.request;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import org.testng.annotations.Test;


public class KeyPartitionProfilerRequestTest {
  @Test
  public void parsesStartWithBothQueryParams() {
    KeyPartitionProfilerRequest req = parse("/key_partition_profiler/myStore_v3/start?duration=60&topK=25");
    assertEquals(req.getStoreVersion(), "myStore_v3");
    assertEquals(req.getStoreName(), "myStore");
    assertEquals(req.getAction(), KeyPartitionProfilerRequest.Action.START);
    assertEquals(req.getDurationMs(), Long.valueOf(60_000L));
    assertEquals(req.getTopK(), Integer.valueOf(25));
  }

  @Test
  public void parsesStartWithoutTopKLeavesItNull() {
    KeyPartitionProfilerRequest req = parse("/key_partition_profiler/myStore_v3/start?duration=30");
    assertEquals(req.getAction(), KeyPartitionProfilerRequest.Action.START);
    assertEquals(req.getDurationMs(), Long.valueOf(30_000L));
    assertNull(req.getTopK(), "topK should be null when not supplied; handler applies the default");
  }

  @Test
  public void parsesStopWithNoQueryParams() {
    KeyPartitionProfilerRequest req = parse("/key_partition_profiler/myStore_v3/stop");
    assertEquals(req.getAction(), KeyPartitionProfilerRequest.Action.STOP);
    assertNull(req.getDurationMs());
    assertNull(req.getTopK());
  }

  @Test
  public void subActionIsCaseInsensitive() {
    KeyPartitionProfilerRequest req = parse("/key_partition_profiler/myStore_v3/START?duration=10");
    assertEquals(req.getAction(), KeyPartitionProfilerRequest.Action.START);
  }

  @Test
  public void rejectsUnknownSubAction() {
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/myStore_v3/bogus"));
  }

  @Test
  public void rejectsNonVersionTopic() {
    // Path segment must look like a Venice version topic (storename_vN).
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/notAVersionTopic/start?duration=10"));
  }

  @Test
  public void rejectsMalformedPathLength() {
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/myStore_v3"));
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/myStore_v3/start/extra"));
  }

  @Test
  public void rejectsNonNumericDuration() {
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/myStore_v3/start?duration=abc"));
  }

  @Test
  public void rejectsNonNumericTopK() {
    assertThrows(VeniceException.class, () -> parse("/key_partition_profiler/myStore_v3/start?duration=10&topK=xyz"));
  }

  private static KeyPartitionProfilerRequest parse(String uri) {
    HttpRequest request = mock(HttpRequest.class);
    doReturn(uri).when(request).uri();
    return KeyPartitionProfilerRequest.parseHttpRequest(request, URI.create(uri));
  }
}
