package com.linkedin.davinci.client;

import java.nio.ByteBuffer;


public abstract class TransformedRecord<K, V> {
  private K key;
  private V value;

  public abstract K getKey();

  public abstract void setKey(K key);

  public abstract byte[] getKeyBytes();

  public abstract V getValue();

  public abstract void setValue(V value);

  public abstract ByteBuffer getValueBytes();

}
