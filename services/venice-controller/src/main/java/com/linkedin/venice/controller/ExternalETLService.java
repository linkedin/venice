package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;


/**
 * Service interface for managing external ETL operations in Venice.
 *
 * <p>This service is used to trigger ETL jobs for stores configured with the ETL strategy
 * {@code EXTERNAL_WITH_VENICE_TRIGGER}. When stores use this strategy, Venice takes responsibility
 * for initiating and managing the lifecycle of ETL jobs, while the actual ETL execution is handled
 * by external services.</p>
 *
 * <p>Implementations of this interface coordinate with external ETL systems to:
 * <ul>
 *   <li>Trigger on demand ETL job creation when needed</li>
 *   <li>Monitor job execution status (To be implemented)</li>
 *   <li>Handle manual or on-demand job deletion and cleanup (To be implemented)</li>
 * </ul></p>
 *
 * @see com.linkedin.venice.meta.VeniceETLStrategy#EXTERNAL_WITH_VENICE_TRIGGER
 */
public interface ExternalETLService {
  public void onboardETL(Store store, Version version);

  public void offboardETL(Store store, Version version);
}
