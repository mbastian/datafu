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
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.avro.Schemas;
import datafu.mr.jobs.AbstractAvroJob;

/**
 * Basic Avro MR job which uses Avro POJO reflection
 * 
 * @author Mathieu Bastian
 */
public class BasicAvroIntermediateObjectJob extends AbstractAvroJob
{
  private static final Schema OUTPUT_SCHEMA;

  static
  {
    OUTPUT_SCHEMA =
        Schemas.createRecordSchema(BasicAvroJob.class,
                                   "Output",
                                   new Field("key", Schema.create(Type.LONG), "key", null),
                                   new Field("count", Schema.create(Type.LONG), "count", null));
  }

  @Override
  protected Schema getOutputSchema()
  {
    return OUTPUT_SCHEMA;
  }

  public static class Map extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<User>, AvroValue<LoginEvent>>
  {
    private final User key = new User();
    private final LoginEvent value = new LoginEvent();

    @Override
    protected void map(AvroKey<GenericRecord> input, NullWritable unused, Context context) throws IOException,
        InterruptedException
    {
      key.id = (Long) input.datum().get("id");
      value.date = 1l;

      context.write(new AvroKey<User>(key), new AvroValue<LoginEvent>(value));
    }
  }

  public static class Reduce extends
      Reducer<AvroKey<User>, AvroValue<LoginEvent>, AvroKey<GenericRecord>, NullWritable>
  {
    private final GenericRecord output = new GenericData.Record(OUTPUT_SCHEMA);

    @Override
    protected void reduce(AvroKey<User> user, Iterable<AvroValue<LoginEvent>> values, Context context) throws IOException,
        InterruptedException
    {
      long count = 0l;
      for (Iterator<AvroValue<LoginEvent>> iterator = values.iterator(); iterator.hasNext();)
      {
        iterator.next();
        count++;
      }
      output.put("key", (long) user.datum().id);
      output.put("count", count);
      context.write(new AvroKey<GenericRecord>(output), null);
    }
  }

  public static class User
  {
    private long id;

  }

  public static class LoginEvent
  {
    private long date;

    public long getDate()
    {
      return date;
    }
  }
}
