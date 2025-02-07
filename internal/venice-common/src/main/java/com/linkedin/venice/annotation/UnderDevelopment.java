package com.linkedin.venice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates that an API (class, method, or field) is under development and is not yet stable.
 * This annotation warns users that the annotated element may change or be removed in future versions.
 */
@Retention(RetentionPolicy.CLASS) // Retained in class files but not available at runtime.
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE, ElementType.FIELD })
public @interface UnderDevelopment {
  /**
   * An optional message providing additional details about the development status.
   */
  String value() default "";
}
