package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterKey;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;


public abstract class VenicePath implements ResourcePath<RouterKey> {
  private final String resourceName;
  private Collection<RouterKey> partitionKeys;
  private final String storeName;
  private int versionNumber;
  private boolean retryRequest = false;

  public VenicePath(String resourceName) {
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    this.versionNumber = Version.parseVersionFromKafkaTopicName(resourceName);
  }

  protected void setPartitionKeys(Collection<RouterKey> keys) {
    this.partitionKeys = keys;
  }

  @Nonnull
  @Override
  public Collection<RouterKey> getPartitionKeys() {
    return this.partitionKeys;
  }

  public int getRequestSize() {
    // The final single-element array is being used in closure since closure can only operate final variables.
    final int[] size = {0};
    getPartitionKeys().stream().forEach(key -> size[0] += key.getBytes().length);

    return size[0];
  }

  public int getVersionNumber(){
    return this.versionNumber;
  }

  @Nonnull
  @Override
  public String getResourceName() {
    return this.resourceName;
  }

  public String getStoreName() {
    return this.storeName;
  }

  @Override
  public void setRetryRequest() {
    this.retryRequest = true;
  }

  public boolean isRetryRequest() {
    return this.retryRequest;
  }

  public abstract RequestType getRequestType();

  public abstract VenicePath substitutePartitionKey(RouterKey s);

  public abstract VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s);

  public abstract HttpUriRequest composeRouterRequest(String storageNodeUri);
}
