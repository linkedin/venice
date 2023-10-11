package com.linkedin.venice.router.streaming;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.Optional;
import org.testng.annotations.Test;


public class VeniceChunkedResponseTest {
  @Test
  public void whetherToSkipMessage() {
    VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
    /** The {@link VeniceChunkedResponse} is only instantiated so that it registers its callback into the handler */
    new VeniceChunkedResponse(
        "storeName",
        RequestType.MULTI_GET_STREAMING,
        mock(ChannelHandlerContext.class),
        chunkedWriteHandler,
        mock(RouterStats.class),
        Optional.empty());
    assertThrows(
        // This used to throw a NPE as part of a previous regression, we want a better error message
        VeniceException.class,
        () -> chunkedWriteHandler.write(mock(ChannelHandlerContext.class), null, mock(ChannelPromise.class)));
  }
}
