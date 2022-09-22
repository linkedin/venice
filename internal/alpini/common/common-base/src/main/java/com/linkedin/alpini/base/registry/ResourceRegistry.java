package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.misc.Time;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A registry to manage {@link Shutdownable} resources.
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class ResourceRegistry {
  /* package */ static final Logger LOG = LogManager.getLogger(ResourceRegistry.class);

  private static final AtomicInteger REGISTRY_ID = new AtomicInteger();
  private static final ConcurrentHashMap<Class<?>, Factory<?>> FACTORIES = new ConcurrentHashMap<>();
  private static final LinkedBlockingDeque<ResourceRegistry.State> GLOBAL_REGISTRIES = new LinkedBlockingDeque<>();
  private static final ReferenceQueue<ResourceRegistry> REFERENCE_QUEUE = new ReferenceQueue<>();
  private static final IdentityHashMap<Reference<? extends ResourceRegistry>, State> REFERENCE_MAP =
      new IdentityHashMap<>();
  private static final AtomicBoolean GLOBAL_SHUTDOWN = new AtomicBoolean();
  private static final Thread GC_THREAD;
  private static long _globalShutdownDelayMillis = 0;

  public static final String SHUTDOWN_THREAD_PREFIX = ResourceRegistry.class.getName() + "-shutdown-";

  /**
   * Factories must implement this interface.
   * @param <R> Type of object returned by the factory.
   */
  public static interface Factory<R extends ShutdownableResource> {
  }

  /**
   * Resources which implement {@link ShutdownFirst} will be shut down during the first phase.
   */
  public static interface ShutdownFirst {
  }

  /**
   * Resources which implement {@link ShutdownLast} will be shut down during the last phase.
   */
  public static interface ShutdownLast {
  }

  /**
   * Resources which implement {@link Sync} will be shut down after preceding resources have finished shutting down
   * and further resources will shut down after it has completed shutting down.
   */
  public static interface Sync {
  }

  private final ConcurrentHashMap<Class<?>, Factory<?>> _proxyInstances = new ConcurrentHashMap<>();
  protected final State _state;

  /**
   * The {@link State} class maintains no reference to the enclosing {@link ResourceRegistry} class so that the
   * the public {@linkplain ResourceRegistry} instance may be garbage collected when its reference is lost.
   */
  private static final class State {
    private final int _registryId = REGISTRY_ID.incrementAndGet();
    private final Throwable _stackTrace;
    private final LinkedBlockingDeque<ShutdownableResource> _shutdownQueue = new LinkedBlockingDeque<>();
    private final AtomicBoolean _registered = new AtomicBoolean();
    private final AtomicBoolean _shutdown = new AtomicBoolean();
    private final CountDownLatch _shutdownLatch = new CountDownLatch(1);
    private final Thread _shutdownThread;
    private int _phase;

    private State(Throwable stackTrace) {
      _stackTrace = stackTrace;
      _shutdownThread = new Thread(State.this::performShutdown, SHUTDOWN_THREAD_PREFIX + _registryId);
      _shutdownThread.setDaemon(true);
    }

    private boolean startShutdown() {
      if (!_shutdown.getAndSet(true)) {
        _shutdownThread.start();
        return true;
      }
      return false;
    }

    public boolean isTerminated() {
      return _shutdownLatch.getCount() == 0;
    }

    /**
     * Vacuums any already-terminated resources.
     */
    public void vacuum() {
      Iterator<? extends ShutdownableResource> it = _shutdownQueue.iterator();
      while (it.hasNext()) {
        if (it.next().isTerminated()) {
          it.remove();
        }
      }
    }

    private void drain(LinkedList<ShutdownableResource> list) {
      ShutdownableResource r = null;
      while (!list.isEmpty()) {
        try {
          r = list.remove();
          LOG.info("[{}] Waiting for shutdown: {}", _registryId, r);
          r.waitForShutdown();
        } catch (Throwable e) {
          LOG.warn("[{}] Interrupted during phase {} while waiting for shutdown: {}", _registryId, _phase, r, e);
        }
      }
    }

    private void performShutdown() {
      try {
        LOG.info("[{}] Starting shutdown", _registryId);
        vacuum();

        int startPhase = 2;
        int endPhase = 2;
        {
          Iterator<? extends Shutdownable> test = _shutdownQueue.iterator();
          while (test.hasNext() && !(startPhase == 1 && endPhase == 3)) {
            Shutdownable r = test.next();
            if (r instanceof ShutdownFirst) {
              startPhase = 1;
            }
            if (r instanceof ShutdownLast) {
              endPhase = 3;
            }
          }
        }

        for (_phase = startPhase; _phase <= endPhase; _phase++) {
          LOG.info("[{}] Starting phase {}", _registryId, _phase);
          LinkedList<ShutdownableResource> list = new LinkedList<>();
          Iterator<? extends ShutdownableResource> it = _shutdownQueue.iterator();
          ShutdownableResource r;
          while (it.hasNext()) {
            r = it.next();

            if (_phase == 1 && !(r instanceof ShutdownFirst)) {
              continue;
            }
            if (_phase == 2 && r instanceof ShutdownLast) {
              continue;
            }

            try {
              if (!list.contains(r) && !r.isShutdown()) {
                try {
                  if (r instanceof Sync) {
                    drain(list);
                  }
                  list.add(r);
                } finally {
                  LOG.info("[{}] Starting shutdown: {}", _registryId, r);
                  r.shutdown();

                  if (r instanceof Sync) {
                    drain(list);
                  }
                }
              }
            } catch (Throwable e) {
              LOG.warn("[{}] Interrupted during phase %d while initiating shutdown: {}", _registryId, _phase, r, e);
            }
          }

          drain(list);
        }

        LinkedList<ShutdownableResource> badActors = new LinkedList<>();

        while (!_shutdownQueue.isEmpty()) {
          ShutdownableResource r = _shutdownQueue.peek();
          try {
            if (!r.isShutdown()) {
              LOG.info("[{}] Shutdown: {}", _registryId, r);
              r.shutdown();
            }

            if (!r.isTerminated()) {
              LOG.info("[{}] Waiting for shutdown: {}", _registryId, r);
              r.waitForShutdown();
            }

            if (!r.isTerminated()) {
              if (badActors.contains(r)) {
                LOG.warn("[{}] Resource '{}' does not indicate that it has terminated", _registryId, r);
                badActors.remove(r);
              } else {
                // give it another try to shutdown
                badActors.add(r);
                Thread.yield();
                continue;
              }
            }
          } catch (Throwable e) {
            LOG.warn("[{}] Interrupted while waiting for shutdown: {}", _registryId, r, e);
          }
          _shutdownQueue.removeFirstOccurrence(r);
        }
        LOG.info("[{}] Completed shutdown", _registryId);
        GLOBAL_REGISTRIES.removeFirstOccurrence(this);
      } finally {
        _shutdownLatch.countDown();
      }
    }
  }

  /**
   * Construct a ResourceRegistry class instance. When instances of these {@link ResourceRegistry} are garbage
   * collected when not shut down, they will automatically behave as-if {@link #shutdown()} was invoked and a
   * warning is emitted to the logs.
   */
  public ResourceRegistry() {
    this(true, true);
  }

  /**
   * Construct a ResourceRegistry class instance. Instances constructed with this constructor will not emit any log
   * when it is garbage collected.
   *
   * @param garbageCollectable If true, the {@linkplain ResourceRegistry} will be shut down when no remaining
   *                           references to the {@linkplain ResourceRegistry} remain referenced.
   */
  public ResourceRegistry(boolean garbageCollectable) {
    this(garbageCollectable, false);
  }

  private ResourceRegistry(boolean garbageCollectable, boolean warnOnGarbageCollected) {
    if (!GLOBAL_SHUTDOWN.get()) {
      _state = new State(warnOnGarbageCollected ? new Throwable() : null);

      if (!garbageCollectable) {
        return;
      }

      if (GC_THREAD.isAlive()) {
        PhantomReference<ResourceRegistry> phantom = new PhantomReference<>(this, REFERENCE_QUEUE);
        synchronized (REFERENCE_MAP) {
          REFERENCE_MAP.put(phantom, _state);
        }
        return;
      }
    }
    throw new IllegalStateException();
  }

  /**
   * Checks if the supplied class is a permissable factory class.
   * @param clazz Class to be checked
   * @return true iff class is not null and is a {@link Factory} interface.
   */
  public static boolean isFactoryInterface(Class<?> clazz) {
    return clazz != null && clazz.isInterface() && Factory.class.isAssignableFrom(clazz);
  }

  /**
   * Register the factory with the {@link ResourceRegistry} such that future invocations of {@link #factory(Class)} will
   * return an appropiate proxy for the factory.
   *
   * @param clazz Interface being registered with the {@linkplain ResourceRegistry}.
   * @param factory Factory being registered with the {@linkplain ResourceRegistry}.
   * @param <R> Type of the resource which the factory will return.
   * @param <F> Type of the factory.
   * @return the factory.
   */
  public static <R extends ShutdownableResource, F extends Factory<R>> F registerFactory(Class<F> clazz, F factory) {
    if (!isFactoryInterface(clazz)) {
      throw new IllegalArgumentException(String.format("%s is not an interface which extends Factory", clazz));
    }

    if (!clazz.isAssignableFrom(factory.getClass())) {
      throw new IllegalArgumentException("Factory should be an instance of the Factory class.");
    }

    ArrayList<Class> ptypeList = new ArrayList<Class>();

    for (Type type: clazz.getGenericInterfaces()) {
      if (type instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) type;
        Type rawType = ptype.getRawType();
        if (rawType instanceof Class && Factory.class.isAssignableFrom((Class) rawType)) {
          Type[] arguments = ptype.getActualTypeArguments();
          if (arguments.length == 1 && arguments[0] instanceof Class
              && Shutdownable.class.isAssignableFrom((Class) arguments[0])) {
            ptypeList.add((Class) arguments[0]);
          }
        }
      }
    }

    if (ptypeList.isEmpty()) {
      throw new IllegalArgumentException("Factory<E> missing generic type");
    }

    int numMethods = 0;

    nextMethod: for (Method m: clazz.getMethods()) {
      for (Class<?> ptype: ptypeList) {
        if (ptype.isAssignableFrom(m.getReturnType())) {
          numMethods++;
          continue nextMethod;
        }
      }

      throw new IllegalArgumentException(
          String.format("Factory %s does not return a %s class", clazz, Arrays.toString(ptypeList.toArray())));
    }

    if (numMethods == 0) {
      throw new IllegalArgumentException(String.format("Factory %s does not declare any methods", clazz));
    }

    LOG.debug("Factory {} has {} methods", clazz, numMethods);

    F alreadyRegistered = clazz.cast(FACTORIES.putIfAbsent(clazz, factory));
    if (alreadyRegistered != null && alreadyRegistered != factory) {
      LOG.warn("Factory already registered for {}", clazz.getName());
      return alreadyRegistered;
    }
    return factory;
  }

  /**
   * Return a factory which implements the requested factory class F.
   * @param clazz Interface demanded.
   * @param <R> Resource type which the factory will construct.
   * @param <F> Type of the factory.
   * @return a factory.
   */
  @Nonnull
  public <R extends ShutdownableResource, F extends Factory<R>> F factory(@Nonnull Class<F> clazz) {
    F proxy;
    Factory<?> cached = _proxyInstances.get(Objects.requireNonNull(clazz));
    if (cached == null) {
      if (isFactoryInterface(clazz) && !FACTORIES.containsKey(clazz)) {
        // It is possible that the factory interface is lazy, so force initialization
        try {
          Class.forName(clazz.getName(), true, clazz.getClassLoader());
        } catch (Throwable e) {
          if (e instanceof ExceptionInInitializerError && e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          throw e instanceof Error ? (Error) e : new ExceptionInInitializerError(e);
        }
      }

      final Factory<R> factory = clazz.cast(FACTORIES.get(clazz));

      if (factory == null) {
        throw new IllegalStateException("Factory not registered for " + clazz);
      }

      proxy = clazz.cast(Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] { clazz }, new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
          if (method.getDeclaringClass().equals(Object.class)) {
            return method.invoke(factory, args);
          } else {
            vacuum();
            try {
              return register(ShutdownableResource.class.cast(method.invoke(factory, args)));
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          }
        }
      }));

      cached = _proxyInstances.putIfAbsent(clazz, proxy);
      if (cached != null) {
        proxy = clazz.cast(cached);
      }
    } else {
      proxy = clazz.cast(cached);
    }

    return proxy;
  }

  /**
   * Register a {@link ShutdownableResource} resource with this {@link ResourceRegistry}.
   *
   * @param resource Resource to be registered.
   * @param <R> Type of the resource.
   * @return the resource.
   */
  public <R extends ShutdownableResource> R register(R resource) {
    if (!resource.isShutdown() && !_state._shutdownQueue.contains(resource)) {
      _state._shutdownQueue.addFirst(resource);

      if (_state._registered.compareAndSet(false, true)) {
        GLOBAL_REGISTRIES.add(_state);
      }
    }
    return resource;
  }

  /**
   * Register a {@link Shutdownable} resource with this {@link ResourceRegistry}.
   *
   * @param resource Resource to be registered.
   * @param <R> Type of the resource.
   * @return the resource.
   */
  public <R extends Shutdownable> R register(R resource) {
    register(
        resource instanceof ShutdownableResource
            ? (ShutdownableResource) resource
            : ShutdownableAdapter.wrap(resource));
    return resource;
  }

  /**
   * Register a {@link SyncShutdownable} resource with this {@link ResourceRegistry}.
   *
   * @param resource Resource to be registered.
   * @param <R> Type of the resource.
   * @return the resource.
   */
  public <R extends SyncShutdownable> R register(R resource) {
    register(
        resource instanceof ShutdownableResource
            ? (ShutdownableResource) resource
            : SyncShutdownableAdapter.wrap(resource));
    return resource;
  }

  /**
   * Removes the specified {@link ShutdownableResource} from this {@link ResourceRegistry}.
   *
   * @param resource Resource to be removed.
   * @param <R> Type of the resource.
   */
  public <R extends ShutdownableResource> void remove(R resource) {
    synchronized (_state) {
      Iterator<ShutdownableResource> it = iterator();
      while (it.hasNext()) {
        if (it.next() == resource) {
          it.remove();
          return;
        }
      }
    }
  }

  /**
   * Removes the specified {@link Shutdownable} from this {@link ResourceRegistry}.
   *
   * @param resource Resource to be removed.
   * @param <R> Type of the resource.
   */
  public <R extends Shutdownable> void remove(R resource) {
    if (resource instanceof ShutdownableResource) {
      remove((ShutdownableResource) resource);
    } else {
      synchronized (_state) {
        Iterator<ShutdownableResource> it = iterator();
        while (it.hasNext()) {
          ShutdownableResource r = it.next();
          if (r instanceof ShutdownableAdapter) {
            if (r.toString().equals(resource.toString())) {
              it.remove();
              return;
            }
          }
        }
      }
    }
  }

  /**
   * Removes the specified {@link SyncShutdownable} from this {@link ResourceRegistry}.
   *
   * @param resource Resource to be removed.
   * @param <R> Type of the resource.
   */
  public <R extends SyncShutdownable> void remove(R resource) {
    if (resource instanceof ShutdownableResource) {
      remove((ShutdownableResource) resource);
    } else {
      synchronized (_state) {
        Iterator<ShutdownableResource> it = iterator();
        while (it.hasNext()) {
          ShutdownableResource r = it.next();
          if (r instanceof SyncShutdownableAdapter) {
            if (r.toString().equals(resource.toString())) {
              it.remove();
              return;
            }
          }
        }
      }
    }
  }

  /**
   * Vacuums any already-terminated resources.
   */
  public void vacuum() {
    _state.vacuum();
  }

  /**
   * Starts the shutdown process.
   * It is recommended to perform the actual shutdown activity in a separate thread
   * from the thread that calls shutdown
   */
  public void shutdown() {
    synchronized (_state) {
      if (!_state._shutdown.getAndSet(true)) {
        _state._shutdownThread.start();
        _state.notifyAll();
      }
    }
  }

  /**
   * Waits until another caller calls {@link #shutdown()} on this resource registry.
   * @throws InterruptedException when the wait is interrupted
   */
  public void waitToStartShutdown() throws InterruptedException {
    synchronized (_state) {
      while (!_state._shutdown.get()) {
        _state.wait();
      }
    }
  }

  /**
   * Waits until another caller calls {@link #shutdown()} on this resource registry.
   * @param timeoutInMs Time to wait, in milliseconds.
   * @throws InterruptedException when the wait is interrupted
   * @throws TimeoutException when the operation times out
   */
  public void waitToStartShutdown(long timeoutInMs) throws InterruptedException, TimeoutException {
    long timeout = Time.currentTimeMillis() + timeoutInMs;
    synchronized (_state) {
      while (!_state._shutdown.get()) {
        long waitTime = timeout - Time.currentTimeMillis();
        if (waitTime <= 0) {
          throw new TimeoutException();
        }
        _state.wait(waitTime);
      }
    }
  }

  /**
   * Returns <tt>true</tt> if this resource has been shut down.
   *
   * @return <tt>true</tt> if this resource has been shut down
   */
  public final boolean isShutdown() {
    return _state._shutdown.get();
  }

  /**
   * Returns <tt>true</tt> if the resource has completed shutting down.
   * Note that <tt>isTerminated</tt> is never <tt>true</tt> unless
   * <tt>shutdown</tt> was called first.
   *
   * @return <tt>true</tt> if the resource has completed shutting down.
   */
  public final boolean isTerminated() {
    return _state.isTerminated();
  }

  /**
   * Waits for shutdown to complete
   * @throws java.lang.InterruptedException when the wait is interrupted
   * @throws java.lang.IllegalStateException when the method is invoked before shutdown has started
   */
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    if (_state._shutdown.get()) {
      _state._shutdownLatch.await();
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * Waits for shutdown to complete with a timeout
   * @param timeoutInMs number of milliseconds to wait before throwing TimeoutException
   * @throws java.lang.InterruptedException when the wait is interrupted
   * @throws java.lang.IllegalStateException when the method is invoked before shutdown has been started
   * @throws java.util.concurrent.TimeoutException when the wait times out
   */
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
    if (_state._shutdown.get()) {
      if (!Time.await(_state._shutdownLatch, timeoutInMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException();
      }
    } else {
      throw new IllegalStateException();
    }
  }

  protected Iterator<ShutdownableResource> iterator() {
    return _state._shutdownQueue.iterator();
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return getClass().getName() + _state._registryId;
  }

  /**
   * Starts shutdown for all {@linkplain ResourceRegistry} instances.
   * @throws java.lang.InterruptedException if the current thread is interrupted while waiting to acquire a lock
   */
  public static void globalShutdown() throws InterruptedException {
    if (_globalShutdownDelayMillis != 0) {
      LOG.info(
          "globalShutdown waiting {} millis before shutting down remaining ResourceRegistries.",
          _globalShutdownDelayMillis);
      Time.sleepInterruptable(_globalShutdownDelayMillis);
    }
    long timeout = Time.currentTimeMillis() + 30000;
    LOG.info("Shutting down remaining ResourceRegistries");

    for (State state: GLOBAL_REGISTRIES) {
      state.startShutdown();
    }

    State state;
    while ((state = GLOBAL_REGISTRIES.poll()) != null) {
      if (!state.isTerminated()) {
        state.startShutdown();
        long remaining = timeout - Time.currentTimeMillis();
        if (remaining > 1000 && !Time.await(state._shutdownLatch, 1000, TimeUnit.MILLISECONDS)) {
          LOG.info("Waiting for shutdown of ResourceRegistry-{}", state._registryId);
          remaining -= 1000;
        }
        if (!Time.await(state._shutdownLatch, Math.min(1, remaining), TimeUnit.MILLISECONDS)) {
          LOG.warn("Shutdown timeout of ResourceRegistry-{}", state._registryId);
        }
      }
    }
  }

  static {
    // Ensure that even if the shutdown of the ResourceRegistry is not called, we shutdown stuff anyways.

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      GLOBAL_SHUTDOWN.set(true);
      try {
        globalShutdown();
      } catch (Throwable e) {
        LOG.error("Exception while shutting down ResourceRegistries", e);
      }
    }, "ResourceRegistryGlobalShutdown"));

    GC_THREAD = new Thread(() -> {
      while (!GLOBAL_SHUTDOWN.get()) {
        try {
          Reference<? extends ResourceRegistry> ref = REFERENCE_QUEUE.remove();
          State state;

          synchronized (REFERENCE_MAP) {
            state = REFERENCE_MAP.remove(ref);
          }

          if (state == null || state._shutdown.get()) {
            continue;
          }

          if (state.startShutdown()) {
            if (state._stackTrace != null) {
              LOG.warn("Garbage collecting ResourceRegistry {}", state._registryId, state._stackTrace);
            } else {
              LOG.debug("Garbage collecting ResourceRegistry {}", state._registryId);
            }
          }
        } catch (Throwable e) {
          LOG.warn("Exception within GC.", e);
        }
      }
    }, "ResourceRegistryGarbageCollector");

    GC_THREAD.setDaemon(true);
    GC_THREAD.start();
  }

  /** Get the delay for global shutdown. globalShutdown will wait this many millis before shutting down. */
  public static long getGlobalShutdownDelayMillis() {
    return _globalShutdownDelayMillis;
  }

  /** Set a delay for global shutdown. globalShutdown will wait this many millis before shutting down. */
  public static void setGlobalShutdownDelayMillis(long globalShutdownDelayMillis) {
    ResourceRegistry._globalShutdownDelayMillis = globalShutdownDelayMillis;
  }
}
