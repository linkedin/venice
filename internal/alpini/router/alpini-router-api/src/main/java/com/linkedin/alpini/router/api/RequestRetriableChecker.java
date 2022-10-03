package com.linkedin.alpini.router.api;

import javax.annotation.Nonnull;


/**
 * @author Gaurav Mishra {@literal <gmishra@linkedin.com>}
 */
@FunctionalInterface
public interface RequestRetriableChecker<P, R, HRS> {
  boolean isRequestRetriable(@Nonnull P path, @Nonnull R role, @Nonnull HRS status);
}
