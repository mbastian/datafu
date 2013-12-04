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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;

/**
 * Basic MR job with multiple input paths
 * <p>
 * The job concats the diffent values for the same key.
 * 
 * @author Mathieu Bastian
 */
public class BasicConcatMultipleInputsJob extends AbstractJob
{

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
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends Mapper> getMapperClass()
  {
    return Mapper.class;
  }

  public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>
  {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException
    {

      StringBuilder stringBuilder = new StringBuilder();
      for (Text val : values)
      {
        stringBuilder.append(val.toString());
      }
      System.out.println("REDUCIN: " + key.get() + " -> " + stringBuilder.toString());
      context.write(key, new Text(stringBuilder.toString()));
    }
  }
}
