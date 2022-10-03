package com.linkedin.alpini.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation for text to display as the header of the "--help" output.
 *
 * @see CLIArgumentParser
 * @author Antony T Curtis <acurtis@linkedin.com>
 * @version $Revision$
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CLIHeader {
  /** Header text. Shown in help / usage screens. */
  String value();

}
