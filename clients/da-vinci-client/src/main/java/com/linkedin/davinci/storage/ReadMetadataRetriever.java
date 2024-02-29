package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;


public interface ReadMetadataRetriever {
  MetadataResponse getMetadata(String storeName);

  ServerCurrentVersionResponse getCurrentVersionResponse(String storeName);
}
