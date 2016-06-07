package com.linkedin.venice.router;

import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by mwise on 4/22/16.
 */
public class ControllerLookupHandler extends SimpleChannelUpstreamHandler {

  private static final String SEP = "/";
  private static final Logger logger = Logger.getLogger(ControllerLookupHandler.class);
  private final RoutingDataRepository routing;

  public ControllerLookupHandler(RoutingDataRepository routing){
    super();
    this.routing = routing;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
    Object msg = event.getMessage();
    Channel ch = event.getChannel();
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;
      String resourceType = new VenicePathParserHelper(req.getUri()).getResourceType(); //may be null
      if (VenicePathParser.TYPE_CONTROLLER.equals(resourceType)){
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        byte[] masterControllerUrl = routing.getMasterController().getUrl().getBytes(StandardCharsets.UTF_8);
        response.setContent(ChannelBuffers.wrappedBuffer(masterControllerUrl));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH, response.getContent().readableBytes());

        ChannelFuture f = Channels.future(ch);
        f.addListener(ChannelFutureListener.CLOSE);
        Channels.write(ctx, f, response);
      } else {
        ctx.sendUpstream(event);
        return;
      }
    }
  }
}
