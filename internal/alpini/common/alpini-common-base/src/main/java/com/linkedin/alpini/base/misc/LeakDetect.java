package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.alpini.base.concurrency.RunOnce;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * General purpose object reference leak detector with recovery.
 *
 * <p>A reference to the phantom must still be maintained somewhere... For example: {@code
 *
 *     private final AtomicReference<PhantomReference<AsyncFutureListener<?>>> _phantom = new AtomicReference<>();
 *
 * }
 *
 * <p>Then using the {@linkplain LeakDetect} tool is as simple as: {@code
 *
 * PhantomReference<AsyncFutureListener<?>> phantom = LeakDetect
 *     .newReference(responseListener, () -> ctx.executor().execute(() ->
 *         promise.setSuccess(internalBuildErrorResponse(request, new IllegalStateException("Reference lost")))));
 *
 * promise.addListener(future -> {
 *   phantom.clear();
 *   _phantom.compareAndSet(phantom, null);
 * });
 * _phantom.lazySet(phantom);
 *
 * }
 *
 * <p>Note that the recovery should not reference any object which may hold a reference to the object which is being
 * monitored for leaks.
 *
 * @author Created by acurtis on 5/9/17.
 */
public final class LeakDetect {
  private static final Logger LOG = LogManager.getLogger(LeakDetect.class);

  private static final ExecutorService EXECUTOR =
      Executors.newSingleThreadExecutor(new NamedThreadFactory("LeakDetectRecover") {
        @Override
        protected Thread init(Thread t) {
          Thread thd = super.init(t);
          thd.setDaemon(true);
          return thd;
        }
      });

  private static final ReferenceQueue[] REFERENCE_QUEUE = new ReferenceQueue[8];

  private static final List<Thread> LEAK_DETECT_THREADS = Collections.unmodifiableList(
      IntStream.range(0, REFERENCE_QUEUE.length)
          .mapToObj(id -> new Thread(() -> leakDetectTask(initReferenceQueue(id)), "LeakDetect-" + (id + 1)))
          .collect(Collectors.toList()));

  private static final ThreadLocal<ReferenceQueue> THREAD_REFERENCE_QUEUE =
      ThreadLocal.withInitial(LeakDetect::randomQueue);

  private static ReferenceQueue initReferenceQueue(int id) {
    REFERENCE_QUEUE[id] = new ReferenceQueue();
    return REFERENCE_QUEUE[id];
  }

  private static void leakDetectTask(ReferenceQueue referenceQueue) {
    for (;;) {
      try {
        LOG.debug("Starting leak detect thread");
        for (;;) {
          Reference<?> ref = referenceQueue.remove();
          if (ref instanceof Leak) {
            LOG.debug("Leak detected: {}", ref);
            ((Leak) ref).recover();
          }
        }
      } catch (Throwable ex) {
        LOG.fatal("Unexpected exception", ex);
      }
    }
  }

  private static ReferenceQueue randomQueue() {
    return REFERENCE_QUEUE[ThreadLocalRandom.current().nextInt(REFERENCE_QUEUE.length)];
  }

  static {
    LEAK_DETECT_THREADS.forEach(thread -> thread.setDaemon(true));
    LEAK_DETECT_THREADS.forEach(Thread::start);
  }

  private LeakDetect() {
  }

  /**
   * Make a phantom reference for leak detection. Be sure to call {@link PhantomReference#clear()} when the object
   * is no longer considered leaked.
   *
   * @param object Object to monitor.
   * @param recover Task to invoke when a leak occurs.
   * @return Phantom reference.
   */
  public static <T> PhantomReference<T> newReference(T object, Runnable recover) {
    assert LEAK_DETECT_THREADS.stream().allMatch(Thread::isAlive);
    return new Leak<>(object, recover);
  }

  private static class Leak<T> extends PhantomReference<T> {
    private volatile Runnable _recover;

    @SuppressWarnings("unchecked")
    Leak(@Nonnull T referent, @Nonnull Runnable recover) {
      super(referent, THREAD_REFERENCE_QUEUE.get());
      _recover = RunOnce.make(recover);
    }

    void recover() {
      Runnable task = _recover;
      if (task != null) {
        task.run();
      } else {
        LOG.warn("Phantom reference construction incomplete");
        EXECUTOR.execute(this::recover);
      }
    }
  }
}
