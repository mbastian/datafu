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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import datafu.mr.jobs.AbstractAvroJob;

public class BasicAvroMapOnlyOutputObjectJob extends AbstractAvroJob
{

  public static class TheMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<KeyCount>, NullWritable>
  {

    @Override
    public void map(AvroKey<GenericRecord> input, NullWritable value, Context context) throws IOException,
        InterruptedException
    {
      KeyCount output = new KeyCount((Long) input.datum().get("id"), 1l);
      context.write(new AvroKey<KeyCount>(output), NullWritable.get());
    }
  }

  @SuppressWarnings("unused")
  private static class KeyCount
  {
    private final long key;
    private final long count;

    public KeyCount(long key, long count)
    {
      this.key = key;
      this.count = count;
    }
  }
}
