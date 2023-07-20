package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;


public class ChunkedValueManifestContainer {
  private ChunkedValueManifest manifest;

  public ChunkedValueManifestContainer() {
  }

  public void setManifest(ChunkedValueManifest manifest) {
    this.manifest = manifest;
  }

  public ChunkedValueManifest getManifest() {
    return manifest;
  }
}
