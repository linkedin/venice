package com.linkedin.venice.router;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

@ChannelHandler.Sharable
public class VerifySslHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(VerifySslHandler.class);

  /**
   * If the SSL handler is not in the channel pipeline, then return 403
   * otherwise pass the request along the stack.
   *
   * @param ctx
   * @param req
   * @throws IOException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    if (ctx.channel().pipeline().toMap().get("ssl-handler") == null) {
      logger.warn("Got a non-ssl request on what should be an ssl only port at: " + req.uri());
      MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    } else {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }
  }

}
