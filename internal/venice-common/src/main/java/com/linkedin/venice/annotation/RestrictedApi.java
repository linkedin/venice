package com.linkedin.venice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.CLASS) // Retained in class files but not available at runtime.
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE, ElementType.FIELD })
public @interface RestrictedApi {
  /**
   * An optional message providing additional details about the restricted usage.
   */
  String value() default "";
}
