package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import javax.validation.constraints.NotNull;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;


/**
 * Created by mwise on 3/9/16.
 */
public class VeniceVersionFinder {

  private final ReadOnlyStoreRepository metadataRepository;

  public VeniceVersionFinder(@NotNull ReadOnlyStoreRepository metadataRepository){
    this.metadataRepository = metadataRepository;
  }

  public int getVersion(@NotNull String store)
      throws RouterException {
    Store veniceStore = metadataRepository.getStore(store);
    if (null == veniceStore){
      throw new RouterException(HttpResponseStatus.class, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.getCode(),
          "Store: " + store + " does not exist on this cluster", false);
    }
    if (veniceStore.isEnableReads()) {
      return metadataRepository.getStore(store).getCurrentVersion();
    } else {
      throw new RouterException(HttpResponseStatus.class, HttpResponseStatus.FORBIDDEN,
          HttpResponseStatus.FORBIDDEN.getCode(),
          "Could not read from store: " + store + ", because it's disabled from reading.", false);
    }
  }
}
