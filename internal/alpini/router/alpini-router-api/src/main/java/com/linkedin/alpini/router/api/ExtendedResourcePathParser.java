package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.BasicRequest;
import java.util.Collection;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 6/12/17.
 */
public interface ExtendedResourcePathParser<P extends ResourcePath<K>, K, HTTP_REQUEST extends BasicRequest>
    extends ResourcePathParser<P, K> {
  @Nonnull
  default P parseResourceUri(@Nonnull String uri, @Nonnull HTTP_REQUEST request) throws RouterException {
    return parseResourceUri(uri);
  }

  static <P extends ResourcePath<K>, K, HTTP_REQUEST extends BasicRequest> ExtendedResourcePathParser<P, K, HTTP_REQUEST> wrap(
      ResourcePathParser<P, K> pathParser) {
    assert !(pathParser instanceof ExtendedResourcePathParser);
    return new ExtendedResourcePathParser<P, K, HTTP_REQUEST>() {
      @Nonnull
      @Override
      public P parseResourceUri(@Nonnull String uri) throws RouterException {
        return pathParser.parseResourceUri(uri);
      }

      @Nonnull
      @Override
      public P substitutePartitionKey(@Nonnull P path, K s) {
        return pathParser.substitutePartitionKey(path, s);
      }

      @Nonnull
      @Override
      public P substitutePartitionKey(@Nonnull P path, @Nonnull Collection<K> s) {
        return pathParser.substitutePartitionKey(path, s);
      }
    };
  }
}
