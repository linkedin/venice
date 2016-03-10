package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.MetadataRepository;


/**
 * Created by mwise on 3/9/16.
 */
public class VeniceVersionFinder {

  private MetadataRepository metadataRepository;

  public VeniceVersionFinder(MetadataRepository metadataRepository){
    this.metadataRepository = metadataRepository;
  }

  public int getVersion(String store) {
    return metadataRepository.getStore(store).getCurrentVersion();
  }
}
