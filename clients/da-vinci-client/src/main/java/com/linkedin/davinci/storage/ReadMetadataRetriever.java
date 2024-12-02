package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.MetadataByClientResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;


public interface ReadMetadataRetriever {
  MetadataResponse getMetadata(String storeName);

  MetadataByClientResponse getMetadataByClient(String storeName); // TODO PRANAV rename, getStoreProperties,
                                                                  // getStoreMetadata?

  ServerCurrentVersionResponse getCurrentVersionResponse(String storeName);
}
