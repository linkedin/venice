package com.linkedin.venice.controller.converters;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * A general interface to represent response converters that operates on transport specific response {@Link T} to produce
 * a controller specific responesn {@link D}
 * @param <T> Source transport specific response type
 * @param <D> Controller response type
 */
public interface ResponseConverter<T, D extends ControllerResponse> {
  D convert(T response);
}
