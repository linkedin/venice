package com.linkedin.davinci.callback;

public abstract class BytesStreamingCallback {
  public abstract void onRecordReceived(byte[] key, byte[] value);

  public abstract void onCompletion();
}
