package com.linkedin.venice.hooks;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Map;


/**
 * This interface defines a set of lifecycle events for stores and their store-versions.<br>
 * <br>
 * <b>Pre-hooks and post-hooks</b><br>
 * <br>
 * Most events have a pre- and post- variant, all pre-hooks provide an option to control the outcome of the event
 * (e.g. to abort it), while only some post-hooks provide such flow control, and others are intended for informational
 * purposes only.<br>
 * <br>
 * Barring unforeseen bugs or failures, the {@link #postEndOfPushJob(String, String, VeniceProperties, VeniceProperties, PushJobDetailsStatus)}
 * post-hook should always be executed for all push jobs, regardless of whether the job succeeded or not. For all other
 * post-hooks, the invocation of the hook assumes that the corresponding step succeeded.<br>
 * <br>
 * <b>Failures and timeouts</b><br>
 * <br>
 * All exceptions thrown from any of the hooks are swallowed, logged and emitted as metrics. In the case of pre-hooks,
 * exceptions will be treated as an indication to proceed. If a hook implementer wishes to prevent an operation from
 * proceeding if the hook fails or hangs, then it is the implementer's responsibility to wrap the whole hook's logic
 * into a try-catch or similar safeguard, and to return the signal to abort in case of failure.<br>
 * <br>
 * Likewise, the framework executing these hooks is allowed to impose a maximum duration on the hook's runtime, and to
 * proceed forward in case of timeouts, so hooks should not block for extended durations. Note that some hooks allow for
 * deferring their control flow decisions, via {@link StoreVersionLifecycleEventOutcome#WAIT}, in which case the hooks
 * framework will interrogate the hook again after some interval elapsed. This provides a first-class mechanism for
 * stalling the operation in case the hook needs more time to make its decision. If a hook implementer wishes to ensure
 * that stalling behavior, they should prevent the hook implementation from timing out, and return the signal to wait
 * in cases where their custom logic is taking an abnormally long time to finish.<br>
 * <br>
 * <b>Instance creation, state and lifetime</b><br>
 * <br>
 * All {@link StoreLifecycleHooks} classes need to be registered at startup time instances, and will be created by
 * reflection via the {@link StoreLifecycleHooks} constructor. If multiple hooks are registered, then they will be
 * invoked in the order they appear in the config. The hooks framework can, at its discretion, create, reuse and close
 * instances whenever. Related hooks (such as pre- and post-hooks of a certain event type) are not guaranteed to be
 * invoked on the same hook instance, or even in the same JVM. Therefore, any state which a hook instance accumulates
 * should be strictly limited to best-effort optimizations, e.g. establishing a connection to another service, and
 * keeping that connection (or connection pool) open is fine. All state which is significant for the correctness of the
 * hook should be persisted outside the hooks' runtime memory. Likewise, no state should ever be persisted in the JVM's
 * local filesystem.<br>
 * <br>
 * <b>Store-level hooks configs</b><br>
 * <br>
 * The store config contains a bag of arbitrary properties which can be used to override the behavior of hooks. This bag
 * is passed into all hooks. Note that there is just a single bag of properties per store, therefore the configs of all
 * registered hooks must coexist within that bag. As such, namespacing is strongly recommended, so that different hooks
 * don't conflict with each other.<br>
 * <br>
 * <b>Cardinality</b><br>
 * <br>
 * Each hook is annotated with a cardinality, providing a rough indication of how often the hook can be expected to be
 * invoked.<br>
 * <br>
 * <b>Thread safety</b><br>
 * <br>
 * Hook functions can be invoked concurrently and so hook implementations are expected to be thread-safe.<br>
 * <br>
 * However, the hooks framework guarantees to call hooks one at a time for a given store, so even if multiple hooks for
 * a given store simultaneously become eligible to be called, their invocation will be serialized.<br>
 * @see StoreLifecycleEventOutcome
 * @see StoreVersionLifecycleEventOutcome
 */
@Threadsafe
public abstract class StoreLifecycleHooks {
  private final VeniceProperties defaultConfigs;

  public StoreLifecycleHooks(VeniceProperties defaultConfigs) {
    this.defaultConfigs = defaultConfigs;
  }

  protected VeniceProperties getDefaultConfigs() {
    return this.defaultConfigs;
  }

  /**
   * Invoked prior to updating a store's config with a given hook config. If the hook implementation recognizes the
   * config key and the associated config value is valid (e.g. can be parsed or cast to the correct type), then it
   * should return {@link StoreLifecycleEventOutcome#PROCEED}, otherwise it should return
   * {@link StoreLifecycleEventOutcome#ABORT}.<br>
   * <br>
   * Note that there is a single hook config bag shared by all registered hooks, and so config keys should be namespaced
   * in order to avoid collisions (unless a given config key is intended to control similar behavior in many hooks).<br>
   * <br>
   * The hooks framework will reject the store config update if all registered hooks return the ABORT signal, and will
   * accept it if at least one registered hook returns the PROCEED signal.<br>
   * <br>
   * Cardinality: once per config key/value pair included in a store update command.
   */
  public StoreLifecycleEventOutcome validateHookConfig(
      String clusterName,
      String storeName,
      String configKey,
      String configValue) {
    return StoreLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked prior to starting a new job. The hook has the option of aborting the job.<br>
   * <br>
   * N.B.: this hook returns a {@link StoreLifecycleEventOutcome}, and not a {@link StoreVersionLifecycleEventOutcome},
   * because the new store-version is not created yet at the start of the push job.<br>
   * <br>
   * Cardinality: once per push job.
   */
  public StoreLifecycleEventOutcome preStartOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties storeHooksConfigs) {
    return StoreLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after starting a new job (i.e. if all {@link #preStartOfPushJob(String, String, VeniceProperties)} hooks
   * succeeded).<br>
   * <br>
   * Cardinality: once per push job, assuming no previous failures.
   */
  public void postStartOfPushJob(String clusterName, String storeName, VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to registering a new schema, but after standard checks already succeeded. The hook has the option of
   * aborting the registration (which causes the whole push job to abort as well, if this is happening as part of a
   * job's auto-registration step).<br>
   * <br>
   * Cardinality: once per schema registration attempt.
   */
  public StoreLifecycleEventOutcome preSchemaRegistration(
      String clusterName,
      String storeName,
      String newSchema,
      Map<Integer, String> existingSchemas,
      VeniceProperties storeHooksConfigs) {
    return StoreLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after registering a new schema.<br>
   * <br>
   * Cardinality: once per successful schema registration.
   */
  public void postSchemaRegistration(
      String clusterName,
      String storeName,
      String newSchema,
      Map<Integer, String> existingSchemas,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to creating a store-version in a given region.<br>
   * <br>
   * Cardinality: once per store-version creation attempt per region, assuming no previous failures.
   */
  public StoreVersionLifecycleEventOutcome preStoreVersionCreation(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after creating a store-version in a given region, which means the following actions succeeded:<br>
   * <br>
   * - All {@link #preStoreVersionCreation(String, String, int, String, Lazy, VeniceProperties)} hooks.<br>
   * - Creation of the store-version's dedicated resources (pub sub topic, Helix resource).<br>
   * - Server replicas have begun ingesting.<br>
   * <br>
   * Cardinality: once per successful store-version creation per region.
   */
  public void postStoreVersionCreation(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to informing Da Vinci Clients about starting to ingest a new store-version.<br>
   * <br>
   * Cardinality: once per store-version per region.
   */
  public StoreVersionLifecycleEventOutcome preStartOfStoreVersionIngestionForDaVinci(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after informing Da Vinci Clients that they may start to ingest a new store-version.<br>
   * <br>
   * It is important to note that:<br>
   * <br>
   * 1. DVC will not necessarily have started ingesting when this hook is invoked, since that process happens
   *    asynchronously after notifying.<br>
   * <br>
   * 2. DVC will proceed to swap for serving reads on a per-instance basis as soon the ingestion completes. At this time
   *    there is no support for deferring the swap until all instances of a region have completed (as is the case for
   *    servers).<br>
   * <br>
   * Cardinality: once per store-version per region.
   */
  public StoreVersionLifecycleEventOutcome postStartOfStoreVersionIngestionForDaVinci(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after all leader replicas of a store-version within a single region have completed, which means the data
   * replication to that region is done.<br>
   * <br>
   * Cardinality: once per successfully-replicated store-version per region.
   */
  public void postStoreVersionLeaderReplication(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to swapping read traffic for servers. Specifically, this means:<br>
   * <br>
   * - Leader replication is complete.<br>
   * - A sufficient number of followers have completed their ingestion.<br>
   * - All online Da Vinci Client instances have already swapped their reads to the new store-version.<br>
   * <br>
   * Cardinality: once per store-version per region which is ready to swap.
   */
  public StoreVersionLifecycleEventOutcome preStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after swapping read traffic for servers.<br>
   * <br>
   * Cardinality: once per store-version per region which has successfully swapped.
   */
  public StoreVersionLifecycleEventOutcome postStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked prior to ending a job. Specifically, this means that the new store-version was swapped in all regions. This
   * is a final chance to control the job and its associated store-version prior to termination.<br>
   * <br>
   * Cardinality: once per push job.
   */
  public StoreVersionLifecycleEventOutcome preEndOfPushJob(
      String clusterName,
      String storeName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked when a job ends, either because it completed, had an error, or was killed (as indicated by the
   * {@link PushJobDetailsStatus} parameter).<br>
   * <br>
   * Cardinality: once per push job.
   */
  public void postEndOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties storeHooksConfigs,
      PushJobDetailsStatus terminalStatus) {
  }
}
