package io.netty.handler.timeout;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * If a remote peer is unresponsive due to CPU throttling, GC or otherwise, ForceCloseOnWriteTimeoutHandler closes
 * the connection on write timeout, the time out by default is 30 sec.
 *
 * A close is first attempted on the connection, and is force closed after 5 sec.
 *
 * @author Abhishek Andhavarapu
 */
public class ForceCloseOnWriteTimeoutHandler extends WriteTimeoutHandler {
  private static final Logger LOG = LogManager.getLogger(ForceCloseOnWriteTimeoutHandler.class);

  public ForceCloseOnWriteTimeoutHandler(int timeoutSeconds) {
    super(timeoutSeconds);
  }

  @Override
  protected void writeTimedOut(ChannelHandlerContext ctx) throws Exception {
    /*
      super.writeTimedOut calls close on the channel.
     */
    super.writeTimedOut(ctx);
    /*
      If the channel is not closed with in 5 sec, we force close the connection.
     */
    ScheduledFuture scheduledFuture = ctx.channel().eventLoop().schedule(() -> {
      if (ctx.channel().isActive()) {
        try {
          ctx.channel().unsafe().close(ctx.channel().voidPromise());
          // Clean up
          if (ctx.channel().closeFuture() instanceof ChannelPromise) {
            ((ChannelPromise) ctx.channel().closeFuture()).trySuccess();
          }
        } catch (Exception ex) {
          LOG.error("Force closing connection to {} due to write timeout", ctx.channel());
        }
      }
    }, 5000, TimeUnit.MILLISECONDS);

    // Cancel the schedule, if the close succeds
    ctx.channel().closeFuture().addListener((future) -> scheduledFuture.cancel(true));
  }
}
