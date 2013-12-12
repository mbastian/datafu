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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class LatestExpansionFunction
{
  private final Logger log;
  public static final String LATEST_SUFFIX = "#LATEST";
  private final FileSystem fs;

  public LatestExpansionFunction(final FileSystem fs, Logger log)
  {
    this.log = log;
    this.fs = fs;
  }

  public String apply(String path)
  {
    if (path.contains(LATEST_SUFFIX))
    {
      String actualPath = path.substring(0, path.indexOf(LATEST_SUFFIX));
      String suffix = path.substring(path.indexOf(LATEST_SUFFIX)).replaceAll(LATEST_SUFFIX, "");

      FileStatus[] files;
      try
      {
        files = fs.listStatus(new Path(actualPath));
      }
      catch (IOException e)
      {
        final String message = String.format("Exception when looking for expansion of %s", LATEST_SUFFIX);

        log.error(message, e);
        throw new RuntimeException(message, e);
      }

      List<FileStatus> filtered = new ArrayList<FileStatus>();
      for (FileStatus fs : files)
      {
        String name = fs.getPath().getName();
        if (!name.startsWith("_") && !name.startsWith("."))
        {
          filtered.add(fs);
        }
      }
      files = filtered.toArray(new FileStatus[0]);

      if (files.length == 0)
      {
        throw new RuntimeException(String.format("No files found under path[%s] when resolving path[%s]. fs[%s]",
                                                 actualPath,
                                                 path,
                                                 fs));
      }

      Arrays.sort(files, new Comparator<FileStatus>()
      {
        @Override
        public int compare(FileStatus o1, FileStatus o2)
        {
          return o1.getPath().getName().compareTo(o2.getPath().getName());
        }
      });
      return files[files.length - 1].getPath().toUri().getPath() + suffix;
    }

    return path;
  }
}
