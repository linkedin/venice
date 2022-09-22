package com.linkedin.alpini.netty4.pool;

/*
* This listener is designed to track the http2 ping calls.
*
* @author Binbing Hou <bhou@linkein.com>
* */
public interface Http2PingCallListener {
  boolean isCallComplete(long pingId);

  void callStart(long pingId, long timeNanos);

  void callClose(long pingId, long timeNanos);

  void callCloseWithError(long pingId, long timeNanos);
}
