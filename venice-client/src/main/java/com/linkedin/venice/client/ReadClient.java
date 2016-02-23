package com.linkedin.venice.client;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;


/**
 * TODO: ReadClient creates a new factory and connection for every read.  Can we reuse them?
 */
public class ReadClient {
  ChannelFuture connection;
  ChannelFactory factory;
  ResponseWrapper response = new ResponseWrapper();

  public byte[] doRead(String host, int port, GetRequestObject request){
    factory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new DaemonThreadFactory("venice-netty-boss")),
        Executors.newCachedThreadPool(new DaemonThreadFactory("venice-netty-worker"))
    );
    ClientBootstrap bootstrap = new ClientBootstrap(factory);

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(new ReadHandler(request.serialize(), response));
      }
    });

    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);

    connection = bootstrap.connect(new InetSocketAddress(host,port));

    // TODO: make this actually work in a proper async way
    // maybe sleep for length of timeout, but passing a callback to the ReadHandler to interupt the thread when the response is written?
    while (response == null){
      try {
        Thread.sleep(10);
        System.out.println("Waiting for response");
      } catch (InterruptedException e) {
        break;
      }
    }
    disconnect();
    return response.getResponse();
  }

  public void disconnect(){
    connection.getChannel().getCloseFuture().awaitUninterruptibly();
    factory.releaseExternalResources();
  }
}
