/*
 * Copyright 2008-2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.venice;

/**
 * Base exception that all other Venice exceptions extend.
 *
 */
public class VeniceException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public VeniceException() {
    super();
  }

  public VeniceException(String s, Throwable t) {
    super(s, t);
  }

  public VeniceException(String s) {
    super(s);
  }

  public VeniceException(Throwable t) {
    super(t);
  }

}
