package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.MetadataWithStorePropertiesResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;


public interface ReadMetadataRetriever {
  MetadataResponse getMetadata(String storeName);

  MetadataWithStorePropertiesResponse getMetadataWithStoreProperties(String storeName);

  ServerCurrentVersionResponse getCurrentVersionResponse(String storeName);
}
