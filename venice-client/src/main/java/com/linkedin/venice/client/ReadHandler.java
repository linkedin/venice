package com.linkedin.venice.client;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.message.GetResponseObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;


/**
 * Created by mwise on 1/15/16.
 */
public class ReadHandler extends SimpleChannelHandler {

  private byte[] bytesToSend;
  private ResponseWrapper response;
  private final ChannelBuffer buf = dynamicBuffer();

  public ReadHandler(byte[] bytesToSend, ResponseWrapper response){
    super();
    this.bytesToSend = bytesToSend;
    this.response = response;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e){
    //This should get split out into a decoder which can be independantly testable
    ChannelBuffer m = (ChannelBuffer) e.getMessage();
    buf.writeBytes(m);
    if (buf.readableBytes() < GetResponseObject.HEADER_SIZE){
      return;
    }
    byte[] header = new byte[GetResponseObject.HEADER_SIZE];
    buf.getBytes(buf.readerIndex(), header);
    int payloadSize = GetResponseObject.parseSize(header);
    if (buf.readableBytes() < payloadSize){
      return;
    }
    byte[] payload = new byte[payloadSize];
    buf.readBytes(payload);
    GetResponseObject responseObject = GetResponseObject.deserialize(payload);
    response.setResponse(responseObject.getValue());
    e.getChannel().close();
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e){
    ChannelBuffer responseContent = dynamicBuffer();
    responseContent.writeBytes(bytesToSend);
    e.getChannel().write(responseContent);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e){
    e.getCause().printStackTrace();
    e.getChannel().close();
  }

}
