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
package datafu.mr.jobs;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class DiscoveryHelper
{

  private static final Logger _log = Logger.getLogger(DiscoveryHelper.class);

  @SuppressWarnings("rawtypes")
  public static Class<? extends Mapper> getMapperClass(AbstractJob job)
  {
    if (job.getMapperClass() != null)
    {
      return job.getMapperClass();
    }
    else
    {
      Class<? extends Mapper> c = getNestedClass(job.getClass(), Mapper.class);
      if (c != null)
      {
        _log.info(String.format("Discovered mapper class %s from %s", c.getName(), job.getClass().getName()));
        return c;
      }
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  public static Class<? extends Reducer> getReducerClass(AbstractJob job)
  {
    if (job.getReducerClass() != null)
    {
      return job.getReducerClass();
    }
    else
    {
      Class<? extends Reducer> c = getNestedClass(job.getClass(), Reducer.class);
      if (c != null)
      {
        _log.info(String.format("Discovered reducer class %s from %s", c.getName(), job.getClass().getName()));
        return c;
      }
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<? extends T> getNestedClass(Class<?> c, Class<T> match)
  {
    Class<? extends T> res = null;
    for (Class<?> cls : c.getDeclaredClasses())
    {
      if (match.isAssignableFrom(cls))
      {
        if (res != null)
        {
          throw new RuntimeException("The class of type " + match.getSimpleName() + " can't be discovered in "
              + c.getName() + " because there are multiple matches");
        }
        res = (Class<? extends T>) cls;
      }
    }
    return res;
  }
}
