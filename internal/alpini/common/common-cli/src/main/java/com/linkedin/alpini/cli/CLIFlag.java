/*
 * $Id$
 */
package com.linkedin.alpini.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation for a boolean command line option without an argument, like "--exit-on-error".
 *
 * @see CLIArgumentParser
 * @author Jemiah Westerman <jwesterman@linkedin.com>
 * @version $Revision$
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CLIFlag {
  /** A short sentence description of the option. Shown in help / usage screens. */
  String description();

  /** Long name for the option. If not provided, one will be auto-generated from the field name. */
  String longOpt() default CLIArgumentParser.NO_DEFAULT_STRING;

  /** Short name for the option. If not provided, the option will not have a short name. */
  char shortOpt() default CLIArgumentParser.NO_DEFAULT_CHAR;
}
