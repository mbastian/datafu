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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import datafu.mr.avro.Schemas;
import datafu.mr.jobs.AbstractAvroJob;

public class BasicAvroMapOnlyJob extends AbstractAvroJob
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
  public Schema getOutputSchema()
  {
    return OUTPUT_SCHEMA;
  }

  public static class TheMapper extends
      Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable>
  {

    private final GenericRecord key = new GenericData.Record(OUTPUT_SCHEMA);

    @Override
    public void map(AvroKey<GenericRecord> input, NullWritable value, Context context) throws IOException,
        InterruptedException
    {
      key.put("key", input.datum().get("id"));
      key.put("count", 1l);
      context.write(new AvroKey<GenericRecord>(key), NullWritable.get());
    }
  }
}
