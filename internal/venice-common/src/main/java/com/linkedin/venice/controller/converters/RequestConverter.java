package com.linkedin.venice.controller.converters;

import com.linkedin.venice.controller.requests.ControllerRequest;


/**
 * A general interface to represent request converters that operates on {@Link T} to produce a transport specific
 * request {@link D}
 * @param <T> Source controller request type
 * @param <D> Transport specific request type
 */
@FunctionalInterface
public interface RequestConverter<T extends ControllerRequest, D> {
  D convert(T request);
}
