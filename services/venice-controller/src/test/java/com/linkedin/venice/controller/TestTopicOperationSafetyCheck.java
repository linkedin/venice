package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;


/**
 * Unit tests for the multi-region topic-operation safety check ({@link Admin#checkTopicOperationSafety(String)}).
 *
 * <p>The parent-controller decision logic lives in {@link ParentVersionOrchestrator}; these tests spy the orchestrator
 * and stub the per-region version fan-outs ({@code getCurrentVersionsForMultiColos} / {@code ...Future...} /
 * {@code ...Backup...}) so they exercise the block/allow branches in isolation. The cross-fabric fan-out plumbing and
 * end-to-end behavior are covered by integration tests.</p>
 */
public class TestTopicOperationSafetyCheck {
  private static final String STORE = "testStore";
  private static final String CLUSTER = "test-cluster";
  private static final String VT_V2 = STORE + "_v2"; // version topic for version 2
  private static final String RT = STORE + "_rt"; // real-time topic

  /**
   * Build a spied orchestrator whose store-config resolution returns {@code cluster} (or no owning store when
   * {@code cluster == null}) and whose per-region version fan-outs return the supplied maps.
   */
  private ParentVersionOrchestrator orchestrator(
      String cluster,
      Map<String, Integer> current,
      Map<String, String> future,
      Map<String, String> backup) {
    VeniceParentHelixAdmin parent = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin internal = mock(VeniceHelixAdmin.class);
    when(parent.getVeniceHelixAdmin()).thenReturn(internal);
    HelixReadOnlyStoreConfigRepository repo = mock(HelixReadOnlyStoreConfigRepository.class);
    when(internal.getStoreConfigRepo()).thenReturn(repo);
    if (cluster == null) {
      when(repo.getStoreConfig(STORE)).thenReturn(Optional.empty());
    } else {
      StoreConfig storeConfig = mock(StoreConfig.class);
      when(storeConfig.getCluster()).thenReturn(cluster);
      when(repo.getStoreConfig(STORE)).thenReturn(Optional.of(storeConfig));
    }

    ParentVersionOrchestrator orchestrator = spy(new ParentVersionOrchestrator(parent));
    if (cluster != null) {
      doReturn(current).when(orchestrator).getCurrentVersionsForMultiColos(cluster, STORE);
      doReturn(future).when(orchestrator).getFutureVersionsForMultiColos(cluster, STORE);
      doReturn(backup).when(orchestrator).getBackupVersionsForMultiColos(cluster, STORE);
    }
    return orchestrator;
  }

  private static Map<String, Integer> currentVersions(Object... regionVersionPairs) {
    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < regionVersionPairs.length; i += 2) {
      map.put((String) regionVersionPairs[i], (Integer) regionVersionPairs[i + 1]);
    }
    return map;
  }

  private static Map<String, String> versionStrings(String... regionVersionPairs) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < regionVersionPairs.length; i += 2) {
      map.put(regionVersionPairs[i], regionVersionPairs[i + 1]);
    }
    return map;
  }

  @Test
  public void testBlocksWhenTargetIsCurrentVersionInOneRegion() {
    // region1 still serves v2 while region2 has moved to v3 -> deleting _v2 must be blocked, citing region1 only.
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 2, "region2", 3), versionStrings(), versionStrings());

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(VT_V2);

    assertFalse(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().containsKey("region1"));
    assertFalse(verdict.getBlockingRegions().containsKey("region2"));
    assertEquals(verdict.getCluster(), CLUSTER);
    assertEquals(verdict.getStoreName(), STORE);
  }

  @Test
  public void testAllowsWhenTargetIsDeprecatedInAllRegions() {
    // v2 is neither current (5), future (6), nor backup (4) anywhere -> safe.
    ParentVersionOrchestrator orchestrator = orchestrator(
        CLUSTER,
        currentVersions("region1", 5, "region2", 5),
        versionStrings("region1", "6", "region2", "6"),
        versionStrings("region1", "4", "region2", "4"));

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(VT_V2);

    assertTrue(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().isEmpty());
  }

  @Test
  public void testBlocksWhenTargetIsFutureVersion() {
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 5), versionStrings("region1", "2"), versionStrings());

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(VT_V2);

    assertFalse(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().containsKey("region1"));
  }

  @Test
  public void testBlocksWhenTargetIsBackupVersion() {
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 5), versionStrings(), versionStrings("region1", "2"));

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(VT_V2);

    assertFalse(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().containsKey("region1"));
  }

  @Test
  public void testBlocksWhenAnyRegionIsUnreachable() {
    // region2 returned the IGNORED_CURRENT_VERSION (-1) sentinel -> cannot prove safety -> fail safe and block,
    // even though v2 is not the current version anywhere reachable.
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 5, "region2", -1), versionStrings(), versionStrings());

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(VT_V2);

    assertFalse(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().containsKey("region2"));
    assertFalse(verdict.getBlockingRegions().containsKey("region1"));
  }

  @Test
  public void testAllowsOrphanedTopicWithNoOwningStore() {
    // No store config resolves for the topic -> additive check allows it (legacy behavior preserved).
    ParentVersionOrchestrator orchestrator = orchestrator(null, null, null, null);

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(STORE + "_v9");

    assertTrue(verdict.isAllowed());
  }

  @Test
  public void testBlocksRealTimeTopicWhenStoreHasCurrentVersion() {
    // A real-time topic is shared across versions; any serving version in any region makes it unsafe.
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 3), versionStrings(), versionStrings());

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(RT);

    assertFalse(verdict.isAllowed());
    assertTrue(verdict.getBlockingRegions().containsKey("region1"));
  }

  @Test
  public void testAllowsRealTimeTopicWhenStoreHasNoCurrentVersion() {
    // NON_EXISTING_VERSION (0) in every region -> no serving version -> RT topic is safe.
    ParentVersionOrchestrator orchestrator =
        orchestrator(CLUSTER, currentVersions("region1", 0, "region2", 0), versionStrings(), versionStrings());

    TopicOperationSafetyVerdict verdict = orchestrator.checkTopicOperationSafety(RT);

    assertTrue(verdict.isAllowed());
  }

  @Test
  public void testChildControllerBlocksBecauseItCannotVerifyAllRegions() {
    VeniceHelixAdmin childAdmin = mock(VeniceHelixAdmin.class);
    when(childAdmin.checkTopicOperationSafety(VT_V2)).thenCallRealMethod();
    HelixReadOnlyStoreConfigRepository repo = mock(HelixReadOnlyStoreConfigRepository.class);
    when(childAdmin.getStoreConfigRepo()).thenReturn(repo);
    StoreConfig storeConfig = mock(StoreConfig.class);
    when(storeConfig.getCluster()).thenReturn(CLUSTER);
    when(repo.getStoreConfig(STORE)).thenReturn(Optional.of(storeConfig));

    TopicOperationSafetyVerdict verdict = childAdmin.checkTopicOperationSafety(VT_V2);

    assertFalse(verdict.isAllowed());
    assertEquals(verdict.getCluster(), CLUSTER);
  }

  @Test
  public void testChildControllerAllowsOrphanedTopic() {
    VeniceHelixAdmin childAdmin = mock(VeniceHelixAdmin.class);
    when(childAdmin.checkTopicOperationSafety(STORE + "_v9")).thenCallRealMethod();
    HelixReadOnlyStoreConfigRepository repo = mock(HelixReadOnlyStoreConfigRepository.class);
    when(childAdmin.getStoreConfigRepo()).thenReturn(repo);
    when(repo.getStoreConfig(STORE)).thenReturn(Optional.empty());

    TopicOperationSafetyVerdict verdict = childAdmin.checkTopicOperationSafety(STORE + "_v9");

    assertTrue(verdict.isAllowed());
  }

  @Test
  public void testVerdictFactoryMethods() {
    TopicOperationSafetyVerdict allowed = TopicOperationSafetyVerdict.allowed(VT_V2, STORE, CLUSTER, "ok");
    assertTrue(allowed.isAllowed());
    assertTrue(allowed.getBlockingRegions().isEmpty());
    assertEquals(allowed.getReason(), "ok");

    Map<String, String> blocking = Collections.singletonMap("region1", "current version");
    TopicOperationSafetyVerdict blocked = TopicOperationSafetyVerdict.blocked(VT_V2, STORE, CLUSTER, "nope", blocking);
    assertFalse(blocked.isAllowed());
    assertEquals(blocked.getBlockingRegions(), blocking);
    assertEquals(blocked.getStoreName(), STORE);
  }
}
