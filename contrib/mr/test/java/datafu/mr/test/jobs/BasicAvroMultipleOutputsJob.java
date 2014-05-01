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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.avro.Schemas;
import datafu.mr.jobs.AbstractAvroJob;

public class BasicAvroMultipleOutputsJob extends AbstractAvroJob
{

  public static final Schema OUTPUT_SCHEMA;

  static
  {
    OUTPUT_SCHEMA =
        Schemas.createRecordSchema(BasicAvroJob.class,
                                   "Output",
                                   new Field("key", Schema.create(Type.LONG), "key", null),
                                   new Field("count", Schema.create(Type.LONG), "count", null));
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException
  {
    AvroMultipleOutputs.addNamedOutput(job, "odd", AvroKeyOutputFormat.class, OUTPUT_SCHEMA, null);
    AvroMultipleOutputs.addNamedOutput(job, "even", AvroKeyOutputFormat.class, OUTPUT_SCHEMA, null);
  }

  public static class MOMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, IntWritable, IntWritable>
  {

    @Override
    protected void map(AvroKey<GenericRecord> input, NullWritable unused, Context context) throws IOException,
        InterruptedException
    {
      context.write(new IntWritable(((Long) input.datum().get("id")).intValue()), new IntWritable(1));
    }
  }

  public static class MOReduce extends Reducer<IntWritable, IntWritable, AvroKey<GenericRecord>, NullWritable>
  {
    private AvroMultipleOutputs amos;

    @Override
    public void setup(Context context)
    {
      amos = new AvroMultipleOutputs(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException
    {
      int number = key.get();
      int sum = 0;
      for (IntWritable v : values)
      {
        sum += v.get();
      }

      GenericData.Record res = new GenericData.Record(OUTPUT_SCHEMA);
      res.put("key", (long) number);
      res.put("count", (long) sum);

      if (number % 2 == 0)
      {
        amos.write("even", new AvroKey<GenericRecord>(res), NullWritable.get(), "even" + "/part");
      }
      else
      {
        amos.write("odd", new AvroKey<GenericRecord>(res), NullWritable.get(), "odd" + "/part");
      }
    }

    public void cleanup(Context c) throws IOException,
        InterruptedException
    {
      amos.close();
    }
  }
}
