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
package datafu.mr.test.jobs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;

/**
 * Basic MR job which uses the distributed cache.
 * 
 * @author Mathieu Bastian
 */
public class BasicDistributedCacheJob extends AbstractJob
{
  private final boolean useSymlink;

  public BasicDistributedCacheJob(boolean useSymlink)
  {
    this.useSymlink = useSymlink;
  }

  @Override
  public void setupInputFormat(Job job) throws IOException
  {
    job.setInputFormatClass(SequenceFileInputFormat.class);
  }

  @Override
  public void setupIntermediateFormat(Job job) throws IOException
  {
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException
  {
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends Mapper> getMapperClass()
  {
    return useSymlink ? MapWithSymlink.class : MapWithoutSymlink.class;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends Reducer> getReducerClass()
  {
    return null;
  }

  @Override
  protected Class<?> getMapOutputKeyClass()
  {
    return IntWritable.class;
  }

  @Override
  protected Class<?> getMapOutputValueClass()
  {
    return IntWritable.class;
  }

  @Override
  public List<Path> getDistributedCachePaths()
  {
    return Arrays.asList(new Path[] { new Path(useSymlink ? "/cache#suffix" : "/cache") });
  }

  public static class MapWithSymlink extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>
  {

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      File distributedCacheFile = new File("suffix");
      if (!distributedCacheFile.exists())
      {
        throw new RuntimeException("The distributed cache file '" + distributedCacheFile.getPath() + "' doesn't exist");
      }
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException,
        InterruptedException
    {
    }
  }

  public static class MapWithoutSymlink extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>
  {

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      boolean found = false;
      for (Path p : cacheFiles)
      {
        if (p.getName().equals("cache"))
        {
          found = true;
        }
      }
      if (!found)
      {
        throw new RuntimeException("The distributed cache file doesn't exist");
      }
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException,
        InterruptedException
    {
    }
  }
}
