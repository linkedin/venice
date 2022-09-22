package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 9/7/17.
 */
@ChannelHandler.Sharable
public class SslInitializer extends ChannelInitializer<Channel> {
  public static final SslHandshakeCompletionEvent NO_SSL_HANDSHAKE =
      new SslHandshakeCompletionEvent(ExceptionUtil.withoutStackTrace(new SSLHandshakeException("No SSL")));

  public static boolean isNoSslHandshake(Throwable cause) {
    return NO_SSL_HANDSHAKE.cause() == cause;
  }

  private static final AttributeKey<Boolean> RELEASED_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SslInitializer.class, "released");
  private static final AttributeKey<SSLEngine> SSL_ENGINE_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SslInitializer.class, "sslEngine");
  private static final AttributeKey<Long> SSL_HANDSHAKE_START_TS =
      AttributeKey.valueOf(SslInitializer.class, "sslHandshakeStartTs");

  private static final String SSL_DETECT_NAME = "ssl-detect";
  private static final String SSL_HANDLER_NAME = "ssl-handler";
  private static final String HANDSHAKE_COMPLETE_NAME = "SslInitializerComplete";
  private static final String POST_HANDSHAKE_HANDLER_NAME = "PostHandshakeHandler";

  private static final Logger LOG = LogManager.getLogger(SslInitializer.class);
  private final boolean _sslEnabled;
  private final boolean _requireSSL;
  private final SSLEngineFactory _sslFactory;
  private final ChannelHandler _postHandshakeHandler;
  private boolean _resolveClient;
  private EventExecutorGroup _resolveExecutor;
  private Executor _sslExecutor;
  private int _resolveAttempts;
  private long _resolveBackOffMillis;
  private final Queue<ChannelPromise> _pendingHandshake = new ConcurrentLinkedQueue<>();
  private final Semaphore _handshakeSemaphore = new Semaphore(0);
  private final HandshakeComplete _handshakeComplete = new HandshakeComplete();
  private final HandshakeRelease _handshakeRelease = new HandshakeRelease();
  private final LongAdder _handshakesStarted = new LongAdder();
  private final LongAdder _handshakesSuccessful = new LongAdder();
  private final LongAdder _handshakesFailed = new LongAdder();

  private ResolveByAddress _resolveByAddress = InetAddress::getByAddress;
  private ResolveAllByName _resolveAllByName = InetAddress::getAllByName;

  public SslInitializer(SslFactory sslFactory, boolean requireSSL) {
    this(sslFactory, requireSSL, null);
  }

  public SslInitializer(SslFactory sslFactory, ChannelHandler postHandshakeHandler) {
    this(sslFactory, true, postHandshakeHandler);
  }

  public SslInitializer(SslFactory sslFactory, boolean requireSSL, ChannelHandler postHandshakeHandler) {
    this(SSLEngineFactory.adaptSSLFactory(sslFactory), requireSSL, postHandshakeHandler);
  }

  public SslInitializer(SSLEngineFactory sslFactory, ChannelHandler postHandshakeHandler) {
    this(sslFactory, true, postHandshakeHandler);
  }

  public SslInitializer(SSLEngineFactory sslFactory, boolean requireSSL, ChannelHandler postHandshakeHandler) {
    _sslEnabled = sslFactory != null && sslFactory.isSslEnabled();
    _requireSSL = requireSSL;
    if (_sslEnabled) {
      _sslFactory = sslFactory;
      SSLSessionContext sessionContext = _sslFactory.sessionContext(true);
      LOG.info(
          "factory={} sessionTimeout={} sessionCacheSize={}",
          _sslFactory,
          sessionContext.getSessionTimeout(),
          sessionContext.getSessionCacheSize());
      _postHandshakeHandler = postHandshakeHandler;
    } else {
      _sslFactory = null;
      _postHandshakeHandler = null;
    }
  }

  /**
   * Returns the number of available permits for handshakes. This many handshakes will not be immediately blocked.
   * This would return 0 for when offloading is disabled.
   *
   * @return available permits.
   */
  public int getAvailablePermits() {
    return _handshakeSemaphore.availablePermits();
  }

  /**
   * Returns the number of handshakes which are currently blocked awaiting a permit.
   * This would return 0 for when offloading is disabled.
   *
   * @return blocked handshake count
   */
  public int getPendingHandshakes() {
    return _pendingHandshake.size();
  }

  /**
   * Returns the number of handshakes which have been started.
   *
   * @return number of HELLOs.
   */
  public long getHandshakesStarted() {
    return _handshakesStarted.longValue();
  }

  /**
   * Returns the number of handshakes which had resulted in success.
   * @return count
   */
  public long getHandshakesSuccessful() {
    return _handshakesSuccessful.longValue();
  }

  /**
   * Returns the number of handshakes which had resulted in failure.
   * @return count
   */
  public long getHandshakesFailed() {
    return _handshakesFailed.longValue();
  }

  /**
   * Configure for performing DNS resolution of the client address on an alternate
   * thread pool to avoid and limit DNS execution during SSL handshake from blocking
   * the IO Workers.
   *
   * This method limits the number of concurrent handshakes to be the same number of
   * EventExecutors within the resolveExecutor group.
   *
   * @param resolveExecutor The executor for performing DNS resolution.
   * @param resolveAttempts The number of attempts for each client DNS resolution.
   * @param resolveBackOffMillis The delay between client DNS resolution attempts.
   * @return this
   */
  public SslInitializer enableResolveBeforeSSL(
      @Nonnull EventExecutorGroup resolveExecutor,
      @Nonnegative int resolveAttempts,
      @Nonnegative long resolveBackOffMillis) {
    return enableResolveBeforeSSL(
        resolveExecutor,
        resolveAttempts,
        resolveBackOffMillis,
        Math.toIntExact(StreamSupport.stream(resolveExecutor.spliterator(), false).count()));
  }

  /**
   * Configure for performing DNS resolution of the client address on an alternate
   * thread pool to avoid and limit DNS execution during SSL handshake from blocking
   * the IO Workers.
   *
   * The number of permit may be set to a very high value to effectively disable
   * limitations for concurrent handshakes.
   *
   * @param resolveExecutor The executor for performing DNS resolution.
   * @param resolveAttempts The number of attempts for each client DNS resolution.
   * @param resolveBackOffMillis The delay between client DNS resolution attempts.
   * @param permits Number of concurrent handshakes permitted
   * @return this
   */
  public SslInitializer enableResolveBeforeSSL(
      @Nonnull EventExecutorGroup resolveExecutor,
      @Nonnegative int resolveAttempts,
      @Nonnegative long resolveBackOffMillis,
      @Nonnegative int permits) {
    _resolveExecutor = Objects.requireNonNull(resolveExecutor);
    _resolveAttempts = resolveAttempts;
    _resolveBackOffMillis = resolveBackOffMillis;
    _handshakeSemaphore.release(permits);
    _resolveClient = true;
    return this;
  }

  /**
   * Configure for enabling the {@linkplain SslHandler} to offload handshake compute tasks to an alternate
   * executor.
   *
   * @param executor Executor to perform SslHandler tasks
   * @return this
   */
  public SslInitializer enableSslTaskExecutor(Executor executor) {
    _sslExecutor = Objects.requireNonNull(executor);
    return this;
  }

  protected SSLEngine createSslEngine(ByteBufAllocator allocator, SocketAddress remote) {
    SSLEngine engine;
    if (remote instanceof InetSocketAddress) {
      InetSocketAddress inet = (InetSocketAddress) remote;
      engine = _sslFactory.createSSLEngine(allocator, inet.getHostString(), inet.getPort(), true);
    } else {
      engine = _sslFactory.createSSLEngine(allocator, true);
    }

    engine.setUseClientMode(false);

    SSLParameters sslParameters = _sslFactory.getSSLParameters();
    if (sslParameters != null) {
      Set<String> supportedCiphers = new HashSet<>(Arrays.asList(engine.getSupportedCipherSuites()));
      engine.setEnabledCipherSuites(
          Stream.of(sslParameters.getCipherSuites()).filter(supportedCiphers::contains).toArray(String[]::new));

      // These abstract out a tri-state switch. This level of verbosity is recommended.
      if (sslParameters.getNeedClientAuth()) {
        engine.setNeedClientAuth(true);
      } else if (sslParameters.getWantClientAuth()) {
        engine.setWantClientAuth(true);
      } else {
        engine.setWantClientAuth(false);
      }
    }

    return engine;
  }

  /**
   * Used to assert that the current thread is an EventExecutor within the resolveExecutor group
   */
  private boolean inResolveExecutorEventLoop() {
    return java.util.stream.StreamSupport.stream(_resolveExecutor.spliterator(), false)
        .anyMatch(io.netty.util.concurrent.EventExecutor::inEventLoop);
  }

  @Sharable
  private class HandshakeComplete extends ChannelInboundHandlerAdapter {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof SslHandshakeCompletionEvent) {
        ctx.pipeline().remove(this);
        boolean succeed = ((SslHandshakeCompletionEvent) evt).isSuccess();
        if (succeed) {
          _handshakesSuccessful.increment();
        } else {
          _handshakesFailed.increment();
        }
        if (ctx.channel().hasAttr(SSL_HANDSHAKE_START_TS)) {
          LOG.info(
              "SSL Handshake between {} and router {} in {} ms.",
              ctx.channel().remoteAddress(),
              succeed ? "succeeded" : "failed",
              (Time.nanoTime() - ctx.channel().attr(SSL_HANDSHAKE_START_TS).getAndSet(null)) / 1000000);
        }
      }

      super.userEventTriggered(ctx, evt);
    }
  }

  @Sharable
  final class HandshakeRelease extends HandshakeComplete {
    private void next(ChannelHandlerContext ctx) {
      if (ctx.channel().hasAttr(RELEASED_ATTRIBUTE_KEY)) {
        return;
      }
      ctx.channel().attr(RELEASED_ATTRIBUTE_KEY).set(true);

      for (;;) {
        ChannelPromise promise = _pendingHandshake.poll();
        if (promise == null) {
          _handshakeSemaphore.release();
          break;
        }
        if (promise.trySuccess()) {
          break;
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (!ctx.isRemoved()) {
        next(ctx);
      }
      super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      try {
        super.userEventTriggered(ctx, evt);
      } finally {
        if (evt instanceof SslHandshakeCompletionEvent) {
          next(ctx);
        }
      }
    }
  }

  protected void executorFailure(ChannelPromise promise, RejectedExecutionException ex) {
    promise.setFailure(ex);
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    if (_sslEnabled) {
      class SslDetect extends ByteToMessageDecoder implements Callable<String>, FutureListener<String> {
        private ChannelHandlerContext _channelHandlerContext;
        private ChannelPromise _resolvePromise;
        private int _remainingAttempts = _resolveAttempts;
        private boolean _startResolve;

        private boolean isActive() {
          return !_channelHandlerContext.isRemoved() && _channelHandlerContext.channel().isActive();
        }

        private void initializeSslEngine(SocketAddress remoteAddress) {
          // By constructing the SslEngine on a worker thread, we have offloaded a task which can block when
          // SecureRandom has exhausted its entropy.
          if (!_channelHandlerContext.channel().hasAttr(SSL_ENGINE_ATTRIBUTE_KEY)) {
            _channelHandlerContext.channel()
                .attr(SSL_ENGINE_ATTRIBUTE_KEY)
                .set(createSslEngine(_channelHandlerContext.alloc(), remoteAddress));
          }
        }

        public String call() throws Exception {
          // This is executed on the resolveExecutor thread
          assert inResolveExecutorEventLoop() : "Not in resolveExecutor event executor";

          if (!isActive()) {
            return "closed";
          }
          if (_channelHandlerContext.channel().remoteAddress() instanceof InetSocketAddress) {
            InetSocketAddress remoteAddress = (InetSocketAddress) _channelHandlerContext.channel().remoteAddress();
            InetAddress reverse = _resolveByAddress.getByAddress(remoteAddress.getAddress().getAddress());
            for (InetAddress host: _resolveAllByName.getAllByName(reverse.getHostName())) {
              if (Arrays.equals(host.getAddress(), remoteAddress.getAddress().getAddress())) {
                if (host.getHostName().equals(remoteAddress.getHostName())) {
                  String hostName = remoteAddress.getHostName();
                  initializeSslEngine(remoteAddress);
                  return hostName;
                }
                break;
              }
            }
            throw new UnknownHostException("Remote client failed DNS check: " + reverse);
          } else {
            SocketAddress remoteAddress = _channelHandlerContext.channel().remoteAddress();
            initializeSslEngine(remoteAddress);
            return remoteAddress.toString();
          }
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
          super.handlerAdded(ctx);
          _channelHandlerContext = ctx;
          _resolvePromise = ctx.channel().newPromise();
          _resolvePromise.addListener(this::resolved);
        }

        private void handshakeStarted(ChannelHandlerContext ctx) {
          _handshakesStarted.increment();
          ctx.channel().attr(SSL_HANDSHAKE_START_TS).set(Time.nanoTime());
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
          super.channelReadComplete(ctx);
          if (_startResolve) {
            if (ctx.pipeline().get(HandshakeRelease.class) == null) {
              ChannelPromise promise = ctx.channel().newPromise().addListener(future -> {
                try {
                  _resolveExecutor.submit(this).addListener(this);
                } catch (RejectedExecutionException ex) {
                  executorFailure(_resolvePromise, ex);
                }
              });

              boolean startNow = true;
              boolean semaphoreAcquired = false;
              if (_pendingHandshake.offer(promise)) {
                ctx.pipeline()
                    .addAfter(
                        NettyUtils.executorGroup(ctx.channel()),
                        ctx.name(),
                        HANDSHAKE_COMPLETE_NAME,
                        _handshakeRelease);
                handshakeStarted(ctx);
                semaphoreAcquired = _handshakeSemaphore.tryAcquire();
                startNow = semaphoreAcquired;
              }

              if (startNow) {
                // We do not want short-circuit evaluation because trySuccess must run and there's no advantage for
                // short-circuit evaluation here.
                if (!promise.trySuccess() & semaphoreAcquired) {
                  // This happens when the promise was completed by another thread.
                  _handshakeSemaphore.release();
                }
              }
            }
            _startResolve = false;
          }
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
          if (in.readableBytes() < 5) {
            return;
          }
          ChannelPipeline pipeline = ctx.pipeline();
          if (SslHandler.isEncrypted(in)) {
            if (_resolveClient) {
              ctx.channel().config().setAutoRead(false);
              _startResolve = true;
            } else {
              handshakeStarted(ctx);
              pipeline.addAfter(
                  NettyUtils.executorGroup(ctx.channel()),
                  ctx.name(),
                  HANDSHAKE_COMPLETE_NAME,
                  _handshakeComplete);
              replaceWithSslHandler(pipeline);
            }
          } else {
            if (_requireSSL) {
              ctx.close();
              throw new NotSslRecordException(
                  "Non-SSL data from client " + ctx.channel().remoteAddress() + " : " + ByteBufUtil.hexDump(in));
            }
            // Non SSL
            pipeline.fireUserEventTriggered(NO_SSL_HANDSHAKE);
            pipeline.remove(this);
          }
        }

        /**
         * Replacing the SslDetect instance, which extends ByteToMessageDecoder, will pass all the
         * undecoded bytes to the replacement handler, sslHandler.
         * see {@linkplain ByteToMessageDecoder#handlerRemoved(ChannelHandlerContext)}
         */
        private void replaceWithSslHandler(ChannelPipeline pipeline) {
          SSLEngine engine;
          if (pipeline.channel().hasAttr(SSL_ENGINE_ATTRIBUTE_KEY)) {
            engine = pipeline.channel().attr(SSL_ENGINE_ATTRIBUTE_KEY).get();
          } else {
            engine = createSslEngine(_channelHandlerContext.alloc(), pipeline.channel().remoteAddress());
          }

          SslHandler sslHandler;
          if (_sslExecutor != null) {
            sslHandler = new FusedSslHandler(engine, _sslExecutor);
          } else {
            sslHandler = new FusedSslHandler(engine);
          }

          if (_postHandshakeHandler != null) {
            pipeline.addAfter(
                NettyUtils.executorGroup(pipeline),
                HANDSHAKE_COMPLETE_NAME,
                POST_HANDSHAKE_HANDLER_NAME,
                _postHandshakeHandler);
          }

          pipeline.replace(this, SSL_HANDLER_NAME, sslHandler);
        }

        @Override
        public void operationComplete(Future<String> future) {
          // This is executed on the resolveExecutor thread
          assert inResolveExecutorEventLoop() : "Not in resolveExecutor event executor";

          if (future.isSuccess()) {
            LOG.debug("Resolve successful: {}", future.getNow());
            _resolvePromise.setSuccess();
          } else if (_remainingAttempts-- > 0 && isActive()) {
            LOG.info("Check failure, remaining attempts {}", _remainingAttempts + 1, future.cause());
            long resolveDelay = _resolveBackOffMillis + ThreadLocalRandom.current().nextInt(1000);
            try {
              _resolveExecutor.schedule(this, resolveDelay, TimeUnit.MILLISECONDS).addListener(this);
            } catch (RejectedExecutionException ex) {
              executorFailure(_resolvePromise, ex);
            }
          } else {
            _resolvePromise.setFailure(future.cause());
          }
        }

        private void resolved(Future<? super Void> future) {
          // This is executed on the channel executor thread
          assert _channelHandlerContext.channel().eventLoop().inEventLoop();

          if (!future.isSuccess()) {
            LOG.warn("Resolve failure of client {}", _channelHandlerContext.channel().remoteAddress(), future.cause());
          }
          if (isActive()) {
            replaceWithSslHandler(_channelHandlerContext.pipeline());
            _channelHandlerContext.channel().config().setAutoRead(true);
          }
        }
      }

      // replace the ChannelInitializer with an instance of SslDetect
      ch.pipeline().replace(this, SSL_DETECT_NAME, new SslDetect());
    }
  }

  // Debug interfaces
  interface ResolveByAddress {
    InetAddress getByAddress(byte[] address) throws UnknownHostException;
  }

  interface ResolveAllByName {
    InetAddress[] getAllByName(String host) throws UnknownHostException;
  }

  void setResolveByAddress(ResolveByAddress resolveByAddress) {
    _resolveByAddress = resolveByAddress;
  }

  void setResolveAllByName(ResolveAllByName resolveAllByName) {
    _resolveAllByName = resolveAllByName;
  }
}
