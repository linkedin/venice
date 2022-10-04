package com.linkedin.alpini.base.test;

import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.Shutdownable;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import com.linkedin.alpini.base.registry.SyncShutdownable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public abstract class ResourceRegistryTestBase {
  public static final ThreadLocal<ResourceRegistry> REGISTRY_THREAD_LOCAL = new ThreadLocal<>();
  private final ResourceRegistry _resourceRegistry = new ResourceRegistry();

  public <R extends ShutdownableResource, F extends ResourceRegistry.Factory<R>> F factory(Class<F> clazz) {
    return _resourceRegistry.factory(clazz);
  }

  public <R extends Shutdownable> R register(R resource) {
    return _resourceRegistry.register(resource);
  }

  public <R extends SyncShutdownable> R register(R resource) {
    return _resourceRegistry.register(resource);
  }

  @BeforeMethod(alwaysRun = true)
  public void setResourceRegistryThreadLocal() {
    REGISTRY_THREAD_LOCAL.set(_resourceRegistry);
  }

  @AfterMethod(alwaysRun = true)
  public void removeResourceRegistryThreadLocal() {
    REGISTRY_THREAD_LOCAL.remove();
  }

  @AfterClass(alwaysRun = true)
  public void shutdownResourceRegistry() throws InterruptedException {
    _resourceRegistry.shutdown();
    _resourceRegistry.waitForShutdown();
  }
}
