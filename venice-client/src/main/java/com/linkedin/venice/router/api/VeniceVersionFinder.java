package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.MetadataRepository;
import javax.validation.constraints.NotNull;


/**
 * Created by mwise on 3/9/16.
 */
public class VeniceVersionFinder {

  private final MetadataRepository metadataRepository;

  public VeniceVersionFinder(@NotNull MetadataRepository metadataRepository){
    this.metadataRepository = metadataRepository;
  }

  public int getVersion(@NotNull String store) {
    return metadataRepository.getStore(store).getCurrentVersion();
  }
}
