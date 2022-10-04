package com.linkedin.alpini.router.api;

import java.util.Collection;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ResourcePathParser<P extends ResourcePath<K>, K> {
  @Nonnull
  P parseResourceUri(@Nonnull String uri) throws RouterException;

  @Nonnull
  P substitutePartitionKey(@Nonnull P path, K s);

  @Nonnull
  P substitutePartitionKey(@Nonnull P path, @Nonnull Collection<K> s);
}
