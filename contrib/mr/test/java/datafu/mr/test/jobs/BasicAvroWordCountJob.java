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
import java.util.StringTokenizer;

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
 * Basic Avro MR word count job
 * 
 * @author Mathieu Bastian
 */
public class BasicAvroWordCountJob extends AbstractAvroJob
{

  public static final Schema OUTPUT_SCHEMA = Schemas.createRecordSchema(BasicAvroWordCountJob.class,
                                                                        "Output",
                                                                        new Field("word",
                                                                                  Schema.create(Type.STRING),
                                                                                  "word",
                                                                                  null),
                                                                        new Field("count",
                                                                                  Schema.create(Type.INT),
                                                                                  "count",
                                                                                  null));

  @Override
  public Schema getOutputSchema()
  {
    return OUTPUT_SCHEMA;
  }

  public static class Map extends Mapper<AvroKey<String>, NullWritable, AvroKey<String>, AvroValue<Integer>>
  {
    @Override
    public void map(AvroKey<String> record, NullWritable nullValue, Context context) throws IOException,
        InterruptedException
    {
      String line = record.datum().toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens())
      {
        context.write(new AvroKey<String>(tokenizer.nextToken()), new AvroValue<Integer>(1));
      }
    }
  }

  public static class Reduce extends Reducer<AvroKey<String>, AvroValue<Integer>, AvroKey<GenericRecord>, NullWritable>
  {

    @Override
    public void reduce(AvroKey<String> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException,
        InterruptedException
    {
      int sum = 0;
      for (AvroValue<Integer> val : values)
      {
        sum += val.datum();
      }
      GenericData.Record result = new GenericData.Record(OUTPUT_SCHEMA);
      result.put("word", key.datum());
      result.put("count", sum);

      context.write(new AvroKey<GenericRecord>(result), NullWritable.get());
    }
  }
}
