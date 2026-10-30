package com.linkedin.venice.fastclient.meta;

/**
 * Group-selection sub-strategy used when {@link ClientRoutingStrategyType#HELIX_ASSISTED} is in effect. Mirrors the
 * router's {@code HelixGroupSelectionStrategyEnum} so both sides expose the same choices.
 *
 * <ul>
 *   <li>{@code ROUND_ROBIN} - even distribution across groups ({@link HelixGroupRoutingStrategy}).</li>
 *   <li>{@code LEAST_LOADED} - fewest in-flight requests, latency tie-break
 *       ({@link HelixLeastLoadedGroupRoutingStrategy}).</li>
 *   <li>{@code LATENCY_ADAPTIVE} - latency-weighted skew ({@link HelixLatencyAdaptiveGroupRoutingStrategy}).</li>
 * </ul>
 */
public enum HelixGroupRoutingStrategyType {
  ROUND_ROBIN, LEAST_LOADED, LATENCY_ADAPTIVE
}
