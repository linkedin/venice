package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.StorePropertiesResponse;


public interface ReadMetadataRetriever {
  MetadataResponse getMetadata(String storeName);

  StorePropertiesResponse getStoreProperties(String storeName);

  ServerCurrentVersionResponse getCurrentVersionResponse(String storeName);
}
