package com.linkedin.venice.utils;

import java.io.Closeable;
import java.util.Iterator;


/**
 * An iterator that must be closed after use
 *
 * @param <T> The type being iterated over
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable{
}
