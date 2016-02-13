package com.linkedin.venice.controller.server;

import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.controller.Admin;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.ErrorDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

/**
 * Adapted from: https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
 */
public class Handler extends SimpleChannelInboundHandler<HttpObject> {
  private static final Logger logger = Logger.getLogger(Handler.class.getName());

  public static final String NAME = "storename";
  public static final String PARTITIONS = "partitions";
  public static final String REPLICAS = "replicas";

  private HttpRequest request;
  private boolean readingChunks;
  private final StringBuilder responseContent = new StringBuilder();
  private final String clusterName;
  private final Admin admin;

  private static final HttpDataFactory factory =
      new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE); // Disk if size exceed

  private HttpPostRequestDecoder decoder;

  static {
    DiskFileUpload.deleteOnExitTemporaryFile = true; // should delete file on exit (in normal exit)
    DiskFileUpload.baseDirectory = null; // system temp directory
    DiskAttribute.deleteOnExitTemporaryFile = true; // should delete file on exit (in normal exit)
    DiskAttribute.baseDirectory = null; // system temp directory
  }

  public Handler(String clustername, Admin admin){
    super();
    this.clusterName = clustername;
    this.admin = admin;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (decoder != null) {
      decoder.cleanFiles();
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpRequest) {
      request = (HttpRequest) msg;
      URI uri = new URI(request.getUri());
      if (request.getMethod().equals(HttpMethod.GET)) {
        writeMenu(ctx);
        return;
      }
      if (!uri.getPath().startsWith("/create")) {
        // Write Menu
        writeMenu(ctx);
        return;
      }
      responseContent.setLength(0);

      try {
        decoder = new HttpPostRequestDecoder(factory, request);
      } catch (DecoderException e) {
        handleError("Error creating HttpPostRequestDecoder", e, ctx.channel());
        return;
      }

      readingChunks = HttpHeaders.isTransferEncodingChunked(request);
      if (readingChunks) {
        readingChunks = true;
      }
    }

    // check if the decoder was constructed before
    // if not it handles the form get
    if (decoder != null) {
      if (msg instanceof HttpContent) {
        // New chunk is received
        HttpContent chunk = (HttpContent) msg;
        try {
          decoder.offer(chunk);
        } catch (ErrorDataDecoderException e) {
          handleError("Error decoding chunk", e, ctx.channel());
          return;
        }

        Map<String, String> attributes = parseAttributes();
        responseContent.append("Parsed these parameters:\r\n");
        for (String key : attributes.keySet()){
          responseContent.append("  " + key + ": " + attributes.get(key) + "\r\n");
        }
        if (attributes.containsKey(NAME) && attributes.containsKey(PARTITIONS) && attributes.containsKey(REPLICAS)){
          try {
            VeniceStorePartitionInformation storeInfo =
                new VeniceStorePartitionInformation(
                    attributes.get(NAME),
                    Integer.valueOf(attributes.get(PARTITIONS)),
                    Integer.valueOf(attributes.get(REPLICAS)));
            responseContent.append("Creating Store.\r\n");
            admin.addStore(clusterName, storeInfo);
          } catch (NumberFormatException e) {
            responseContent.append(("Partition count and replica count must be integers"));
          }
        } else {
          responseContent.append("Invalid Store Definition Request!\r\n");
          responseContent.append("Provide store name as: " + NAME + "\r\n");
          responseContent.append("Provide integer partition count as: " + PARTITIONS + "\r\n");
          responseContent.append("Provide integer replica count as: " + REPLICAS + "\r\n");
        }

        if (chunk instanceof LastHttpContent) {
          writeResponse(ctx.channel());
          readingChunks = false;
          reset();
        }
      }
    } else {
      writeResponse(ctx.channel());
    }
  }

  private void reset() {
    request = null;
    decoder.destroy(); // destroy the decoder to release all resources
    decoder = null;
  }

  private Map<String, String> parseAttributes(){
    Map<String, String> bodyAttributes = new HashMap<>();
    try{
      while (decoder.hasNext()){
        InterfaceHttpData data = decoder.next();
        if (data != null && data.getHttpDataType() == HttpDataType.Attribute){
          Attribute attribute = (Attribute) data;
          try {
            String value = attribute.getValue();
            bodyAttributes.put(attribute.getName(), value);
          } catch (IOException e) {
            //log the error but keep parsing
            logger.warn("Failed to parse value for HTTP attribute: " + attribute.getName(), e);
          }
        }
      }
    } catch (EndOfDataDecoderException e1) {
    }
    return bodyAttributes;
  }

  private void writeResponse(Channel channel){
    writeResponse(channel, "text/plain");
  }
  private void writeResponse(Channel channel, String type) {
    // Convert the response content to a ChannelBuffer.
    ByteBuf buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);
    responseContent.setLength(0);

    // Decide whether to close the connection or not.
    boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request.headers().get(CONNECTION))
        || request.getProtocolVersion().equals(HttpVersion.HTTP_1_0)
        && !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request.headers().get(CONNECTION));

    // Build the response object.
    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf);
    response.headers().set(CONTENT_TYPE, type + "; charset=UTF-8");
    response.headers().set(CONTENT_LENGTH, buf.readableBytes());

    // Write the response.
    ChannelFuture future = channel.writeAndFlush(response);
    // Close the connection after the write operation is done if necessary.
    if (close) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private void writeMenu(ChannelHandlerContext ctx) {
    responseContent.setLength(0);
    responseContent.append("<html>");
    responseContent.append("<head>");
    responseContent.append("<title>Venice Store Creator</title>\r\n");
    responseContent.append("</head>\r\n");
    responseContent.append("<body bgcolor=white><style>td{font-size: 12pt;}</style>");

    responseContent.append("<table border=\"0\">");
    responseContent.append("<tr>");
    responseContent.append("<td>");
    responseContent.append("<h1>Venice Store Creator</h1>");
    responseContent.append("</td>");
    responseContent.append("</tr>");
    responseContent.append("</table>\r\n");

    // FORM
    responseContent.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    responseContent.append("<FORM ACTION=\"/create\" METHOD=\"POST\">");
    responseContent.append("<table border=\"0\">");
    responseContent.append("<tr><td>Store Name: <br> <input type=text name=\"storename\" size=20></td></tr>");
    responseContent.append("<tr><td>Partitions: <br> <input type=text name=\"partitions\" size=10>");
    responseContent.append("<tr><td>Replicas: <br> <input type=text name=\"replicas\" size=10>");
    responseContent.append("</td></tr>");
    responseContent.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
    responseContent.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
    responseContent.append("</table></FORM>\r\n");
    responseContent.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");

    responseContent.append("</body>");
    responseContent.append("</html>");

    writeResponse(ctx.channel(), "text/html");
  }

  private void handleError(String message, Throwable cause, Channel channel){
    logger.error(message, cause);
    responseContent.append(cause.getMessage());
    writeResponse(channel);
    channel.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error(responseContent.toString(), cause);
    ctx.channel().close();
  }
}
