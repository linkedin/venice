package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.internal.StringUtil;
import java.util.Optional;


/**
 * Created by acurtis on 3/23/17.
 */
public final class HttpToStringUtils {
  private HttpToStringUtils() {
    throw new IllegalStateException("Never instantiated");
  }

  public static StringBuilder appendFullRequest(StringBuilder buf, FullHttpRequest req) {
    appendFullCommon(buf, req);
    appendInitialLine(buf, req);
    appendHeaders(buf, req.headers());
    appendHeaders(buf, req.trailingHeaders());
    removeLastNewLine(buf);
    return buf;
  }

  static StringBuilder appendFullResponse(StringBuilder buf, FullHttpResponse res) {
    appendFullCommon(buf, res);
    appendInitialLine(buf, res);
    appendHeaders(buf, res.headers());
    appendHeaders(buf, res.trailingHeaders());
    Optional.ofNullable(res)
        .map(FullHttpResponse::content)
        .filter(ByteBuf::isDirect)
        .ifPresent(byteBuf -> buf.append("refCnt: ").append(byteBuf.refCnt()).append(StringUtil.NEWLINE));
    removeLastNewLine(buf);
    return buf;
  }

  public static void appendInitialLine(StringBuilder buf, HttpRequest req) {
    buf.append(req.method());
    buf.append(' ');
    buf.append(req.uri());
    buf.append(' ');
    buf.append(req.protocolVersion());
    buf.append(StringUtil.NEWLINE);
  }

  private static void appendInitialLine(StringBuilder buf, HttpResponse res) {
    buf.append(res.protocolVersion());
    buf.append(' ');
    buf.append(res.status());
    buf.append(StringUtil.NEWLINE);
  }

  public static void appendFullCommon(StringBuilder buf, FullHttpMessage msg) {
    buf.append(StringUtil.simpleClassName(msg));
    buf.append("(decodeResult: ");
    buf.append(msg.decoderResult());
    buf.append(", version: ");
    buf.append(msg.protocolVersion());
    buf.append(", content: ");
    buf.append(msg.content());
    buf.append(')');
    buf.append(StringUtil.NEWLINE);
  }

  public static StringBuilder appendCommon(StringBuilder buf, HttpMessage msg) {
    return buf.append(StringUtil.simpleClassName(msg))
        .append("(decodeResult: ")
        .append(msg.decoderResult())
        .append(", version: ")
        .append(msg.protocolVersion())
        .append(')')
        .append(StringUtil.NEWLINE);
  }

  public static StringBuilder appendHeaders(StringBuilder buf, HttpHeaders headers) {
    headers.forEach(e -> buf.append(e.getKey()).append(": ").append(e.getValue()).append(StringUtil.NEWLINE));
    return buf;
  }

  public static StringBuilder removeLastNewLine(StringBuilder buf) {
    buf.setLength(buf.length() - StringUtil.NEWLINE.length());
    return buf;
  }
}
