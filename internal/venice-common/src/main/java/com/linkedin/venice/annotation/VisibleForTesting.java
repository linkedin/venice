package com.linkedin.venice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates that a method, class, or field is more visible than otherwise necessary
 * for the purpose of testing. This is useful for documenting code and communicating
 * that a specific visibility level (e.g., `public` or `protected`) is intentional
 * for testing purposes.
 */
@Retention(RetentionPolicy.CLASS) // Retained in class files but not available at runtime.
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE, ElementType.FIELD })
public @interface VisibleForTesting {
}
