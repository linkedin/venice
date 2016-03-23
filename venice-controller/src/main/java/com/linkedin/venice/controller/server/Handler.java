package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

/**
 * Adapted from: https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
 */
public class Handler extends SimpleChannelInboundHandler<HttpObject> {
  private static final Logger logger = Logger.getLogger(Handler.class.getName());
  private static ObjectMapper mapper = new ObjectMapper();

  private HttpRequest request;
  private boolean readingChunks;
  private final StringBuilder responseContent = new StringBuilder();
  private Map<String, Object> responseMap = null;
  private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
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


  private void handleGetMethods(URI uri, ChannelHandlerContext ctx){
    List<String> params = new ArrayList<>();
    if (uri.getPath().startsWith(ControllerApiConstants.CREATE_PATH)) { // "/create"
      params.add(ControllerApiConstants.NAME);
      params.add(ControllerApiConstants.STORE_SIZE);
      params.add(ControllerApiConstants.OWNER);
      writeMenu(ctx, "Venice Store Creator", ControllerApiConstants.CREATE_PATH, params);
    } else if (uri.getPath().startsWith(ControllerApiConstants.SETVERSION_PATH)){ // "/setversion"
      params.add(ControllerApiConstants.NAME);
      params.add(ControllerApiConstants.VERSION);
      writeMenu(ctx, "Set Current Version", ControllerApiConstants.SETVERSION_PATH, params);
    } else {
      throw404(ctx);
    }
  }

  private void handlePostMethods(URI uri, ChannelHandlerContext ctx){
    if (uri.getPath().startsWith("/create")) {
      createStore();
    } else if (uri.getPath().startsWith("/setversion")) {
      setActiveVersion();
    } else {
      throw404(ctx);
    }
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
        handleGetMethods(uri, ctx);
        return;
      }

      responseContent.setLength(0);
      responseMap = new HashMap<>();
      responseStatus = HttpResponseStatus.OK;

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

        URI uri = new URI(request.getUri());
        handlePostMethods(uri, ctx);

        if (chunk instanceof LastHttpContent) {
          writeResponse(ctx.channel(), ControllerApiConstants.JSON, responseStatus);
          readingChunks = false;
          reset();
        }
      }
    } else {
      writeResponse(ctx.channel(), ControllerApiConstants.JSON, responseStatus);
    }
  }

  // POST /create
  private void createStore(){
    Map<String, String> attributes = parseAttributes();
    responseContent.append("Parsed these parameters:\r\n");
    responseMap.put("parameters", attributes);
    for (String key : attributes.keySet()){
      responseContent.append("  " + key + ": " + attributes.get(key) + "\r\n");
    }
    if (attributes.containsKey(ControllerApiConstants.NAME) &&
        attributes.containsKey(ControllerApiConstants.STORE_SIZE) &&
        attributes.containsKey(ControllerApiConstants.OWNER) &&
        attributes.get(ControllerApiConstants.NAME).length() > 0 &&
        attributes.get(ControllerApiConstants.OWNER).length() > 0 ){
      try {
        String storeName=attributes.get(ControllerApiConstants.NAME);
        String owner=attributes.get(ControllerApiConstants.OWNER);

        int storeSizeMb = Integer.valueOf(attributes.get(ControllerApiConstants.STORE_SIZE));
        responseContent.append("Creating Store-version.\r\n");
        responseMap.put("action", "creating store-version");
        //create store and versions
        int numberOfPartitions = 3; //TODO configurable datasize per partition
        int numberOfReplicas = 1; //TODO configurable replication factor

        try {
          admin.addStore(clusterName, storeName, owner);
          responseMap.put("store_status", "created");
        } catch (VeniceException e){
          responseMap.put("store_status", e.getMessage()); //Probably already created
          // TODO: use admin to update store with new owner?  Set owner at version level for audit history?
        }
        int version = admin.incrementVersion(clusterName, storeName, numberOfPartitions, numberOfReplicas);
        responseMap.put(ControllerApiConstants.PARTITIONS, numberOfPartitions);
        responseMap.put(ControllerApiConstants.REPLICAS,numberOfReplicas);
        responseMap.put(ControllerApiConstants.VERSION, version);
      } catch (NumberFormatException e) {
        responseContent.append(("Store size must be an integer"));
        responseMap.put("error", ControllerApiConstants.STORE_SIZE + " must be an integer");
        responseStatus = HttpResponseStatus.BAD_REQUEST;
      }
    } else {
      responseContent.append("Invalid Store Definition Request!\r\n");
      responseContent.append("Provide non-empty store name as: " + ControllerApiConstants.NAME + "\r\n");
      responseContent.append("Provide integer store size as: " + ControllerApiConstants.STORE_SIZE + "\r\n");
      responseContent.append("Provide non-empty owner as: " + ControllerApiConstants.OWNER + "\r\n");
      responseMap.put("error", ControllerApiConstants.NAME + "," + ControllerApiConstants.STORE_SIZE + "," + ControllerApiConstants.OWNER + " are required parameters");
      responseStatus = HttpResponseStatus.BAD_REQUEST;
    }
  }

  // POST /setversion
  private void setActiveVersion(){
    Map<String, String> attributes = parseAttributes();
    responseContent.append("Parsed these parameters:\r\n");
    responseMap.put("parameters", attributes);
    for (String key : attributes.keySet()){
      responseContent.append("  " + key + ": " + attributes.get(key) + "\r\n");
    }
    if (attributes.containsKey(ControllerApiConstants.NAME) &&
        attributes.containsKey(ControllerApiConstants.VERSION) &&
        attributes.get(ControllerApiConstants.NAME).length() > 0 &&
        attributes.get(ControllerApiConstants.VERSION).length() > 0 ){
      try {
        String storeName=attributes.get(ControllerApiConstants.NAME);
        Integer version=Integer.parseInt(attributes.get(ControllerApiConstants.VERSION));

        admin.setCurrentVersion(clusterName, storeName, version);

        responseMap.put("store_status", "version set");
        responseMap.put(ControllerApiConstants.VERSION, version);
      } catch (NumberFormatException e) {
        responseContent.append(("Version must be an integer"));
        responseMap.put("error", ControllerApiConstants.VERSION + " must be an integer");
        responseStatus = HttpResponseStatus.BAD_REQUEST;
      } catch (VeniceException e){
        responseContent.append("Error: " + e.getMessage());
        responseMap.put("error", e.getMessage());
        logger.error(e);
        responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      }
    } else {
      responseContent.append("Invalid Store Definition Request!\r\n");
      responseContent.append("Provide non-empty store name as: " + ControllerApiConstants.NAME + "\r\n");
      responseContent.append("Provide integer store version as: " + ControllerApiConstants.VERSION + "\r\n");
      responseMap.put("error", ControllerApiConstants.NAME + "," + ControllerApiConstants.VERSION + " are required parameters");
      responseStatus = HttpResponseStatus.BAD_REQUEST;
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

  private void writeResponse(Channel channel, String type, HttpResponseStatus httpStatus) {
    // Convert the response content to a ChannelBuffer.
    ByteBuf buf;
    if (type.equals(ControllerApiConstants.JSON)){
      try {
        buf = copiedBuffer(mapper.writeValueAsString(responseMap), CharsetUtil.UTF_8);
      } catch (IOException e) {
        buf = copiedBuffer("{\"error\":\"" + e.getMessage() + "\"}", CharsetUtil.UTF_8);
        logger.error(e);
      }
    } else {
      buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);
    }
    responseContent.setLength(0);

    // Decide whether to close the connection or not.
    boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request.headers().get(CONNECTION))
        || request.getProtocolVersion().equals(HttpVersion.HTTP_1_0)
        && !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request.headers().get(CONNECTION));

    // Build the response object.
    DefaultFullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, httpStatus, buf);
    response.headers().set(CONTENT_TYPE, type + "; charset=UTF-8");
    response.headers().set(CONTENT_LENGTH, buf.readableBytes());

    // Write the response.
    ChannelFuture future = channel.writeAndFlush(response);
    // Close the connection after the write operation is done if necessary.
    if (close) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private void writeMenu(ChannelHandlerContext ctx, String title, String postAction, List<String> parameters) {
    responseContent.setLength(0);
    responseContent.append("<html>");
    responseContent.append("<head>");
    responseContent.append("<title>" + title + "</title>\r\n");
    responseContent.append("</head>\r\n");
    responseContent.append("<body bgcolor=white><style>td{font-size: 12pt;}</style>");

    responseContent.append("<table border=\"0\">");
    responseContent.append("<tr>");
    responseContent.append("<td>");
    responseContent.append("<h1>" + title + "</h1>");
    responseContent.append("</td>");
    responseContent.append("</tr>");
    responseContent.append("</table>\r\n");

    // FORM
    responseContent.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    responseContent.append("<FORM ACTION=\"" + postAction + "\" METHOD=\"POST\">");
    responseContent.append("<table border=\"0\">");
    for (String param : parameters){
      responseContent.append("<tr><td>"+param+": <br> <input type=text name=\""+param+"\" size=20></td></tr>");
    }
    responseContent.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
    responseContent.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
    responseContent.append("</table></FORM>\r\n");
    responseContent.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");

    responseContent.append("</body>");
    responseContent.append("</html>");

    writeResponse(ctx.channel(), ControllerApiConstants.TEXT_HTML, HttpResponseStatus.OK);
  }

  private void handleError(String message, Throwable cause, Channel channel){
    logger.error(message, cause);
    responseContent.append(cause.getMessage());
    writeResponse(channel, ControllerApiConstants.TEXT_PLAIN, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    channel.close();
  }

  private void throw404(ChannelHandlerContext ctx){
    responseContent.append("404, resource not found");
    writeResponse(ctx.channel(), ControllerApiConstants.TEXT_PLAIN, HttpResponseStatus.NOT_FOUND);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error(responseContent.toString(), cause);
    ctx.channel().close();
  }
}
