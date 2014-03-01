/**
 * Copyright 2013 LinkedIn, Inc
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
package datafu.mr.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import datafu.mr.fs.PathUtils;

/**
 * Utility to expand a path with the current datetime
 * 
 * @author Mathieu Bastian
 */
public class CurrentExpansionFunction
{
  private final Logger log;
  public static final String CURRENT_SUFFIX = "#CURRENT";

  public CurrentExpansionFunction(Logger log)
  {
    this.log = log;
  }

  public String apply(String path, SimpleDateFormat format)
  {
    if (path.endsWith(CURRENT_SUFFIX))
    {
      String destPath = format.format(new Date());
      path = path.substring(0, path.length() - CURRENT_SUFFIX.length()) + destPath;
    }
    return path;
  }
}
