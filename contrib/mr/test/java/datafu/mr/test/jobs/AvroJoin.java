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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.jobs.AbstractAvroJob;
import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Inner and outer join MR job using Avro
 * 
 * @author Mathieu Bastian
 */
public class AvroJoin extends AbstractAvroJob
{
  private static final Schema MISSING_SCHEMA = Schema.createRecord("MISSING", null, AvroJoin.class.getName(), false);
  static
  {
    MISSING_SCHEMA.setFields(Arrays.asList(new Field[] { new Field("missing", Schema.create(Type.BOOLEAN), null, null) }));
  }
  private static final GenericData.Record MISSING_FLAG = new GenericData.Record(MISSING_SCHEMA);
  static
  {
    MISSING_FLAG.put(0, true);
  }
  private static String CONF_KEYS = "avrojoin.keys";
  private static String CONF_TYPE = "avrojoin.type";
  private static String INTERMEDIATE_SCHEMA = "avrojoin.intermediate.schema";
  private static String OUTPUT_SCHEMA = "avrojoin.output.schema";
  private static String KEY_SCHEMA = "avrojoin.key.schema";

  @Override
  public void init(Configuration conf)
  {
    Map<String, Schema> inputSchemas = null;
    try
    {
      inputSchemas = getInputSchemas();
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }

    // Key schema
    String[] keys = conf.get(CONF_KEYS).split(",");
    HashMap<String, Schema> keySchemas = new HashMap<String, Schema>();
    List<Field> keyFields = new ArrayList<Field>();
    for (int i = 0; i < keys.length; i++)
    {
      keys[i] = keys[i].trim();

      Schema lastFieldSchema = null;
      for (Schema s : inputSchemas.values())
      {
        if (s.getField(keys[i]) == null)
        {
          throw new IllegalArgumentException("The input schema " + s + " doesn't have the '" + keys[i] + "' key");
        }
        Schema fieldSchema = s.getField(keys[i]).schema();
        if (lastFieldSchema == null)
        {
          lastFieldSchema = fieldSchema;
          keySchemas.put(keys[i], fieldSchema);
        }
        else if (!lastFieldSchema.equals(fieldSchema))
        {
          throw new IllegalArgumentException("The input schema " + s + " doesn't match the schema " + lastFieldSchema
              + " for the '" + keys[i] + "' key");
        }
      }
      keyFields.add(new Field(keys[i], lastFieldSchema, "", null));
    }
    Schema keySchema = Schema.createRecord("key_schema", null, null, false);
    keySchema.setFields(keyFields);
    conf.set(KEY_SCHEMA, keySchema.toString());

    // Output schema
    List<Field> outputFields = new ArrayList<Schema.Field>();
    for (Schema inputSchema : inputSchemas.values())
    {
      for (Field field : inputSchema.getFields())
      {
        if (!keySchemas.containsKey(field.name()))
        {
          List<Schema> unionSchemaList = new ArrayList<Schema>();
          unionSchemaList.add(Schema.create(Type.NULL));
          unionSchemaList.add(field.schema());
          Schema unionSchema = Schema.createUnion(unionSchemaList);
          outputFields.add(new Field(field.name(), unionSchema, field.doc(), field.defaultValue(), field.order()));
        }
      }
    }
    for (Map.Entry<String, Schema> entry : keySchemas.entrySet())
    {
      outputFields.add(new Field(entry.getKey(), entry.getValue(), "", null));
    }
    Schema outputSchema = Schema.createRecord("output_schema", null, null, false);
    outputSchema.setFields(outputFields);
    conf.set(OUTPUT_SCHEMA, outputSchema.toString());

    // Intermediate schema
    List<Field> intermediateFields = new ArrayList<Schema.Field>();
    for (Schema inputSchema : inputSchemas.values())
    {
      for (Field field : inputSchema.getFields())
      {
        if (!keySchemas.containsKey(field.name()))
        {
          List<Schema> unionSchemaList = new ArrayList<Schema>();
          unionSchemaList.add(Schema.create(Type.NULL));
          unionSchemaList.add(MISSING_SCHEMA);
          unionSchemaList.add(field.schema());
          Schema unionSchema = Schema.createUnion(unionSchemaList);
          intermediateFields.add(new Field(field.name(), unionSchema, field.doc(), field.defaultValue(), field.order()));
        }
      }
    }
    Schema intermediateSchema = Schema.createRecord("intermediate_schema", null, null, false);
    intermediateSchema.setFields(intermediateFields);
    conf.set(INTERMEDIATE_SCHEMA, intermediateSchema.toString());
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends Reducer> getCombinerClass()
  {
    return CombineJoin.class;
  }

  @Override
  public Schema getMapOutputKeySchema()
  {
    return new Schema.Parser().parse(getConf().get(KEY_SCHEMA));
  }

  @Override
  public Schema getMapOutputValueSchema()
  {
    return new Schema.Parser().parse(getConf().get(INTERMEDIATE_SCHEMA));
  }

  @Override
  public Schema getOutputSchema()
  {
    return new Schema.Parser().parse(getConf().get(OUTPUT_SCHEMA));
  }

  public static class MapJoin extends
      Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>>
  {
    private GenericRecord k;
    private GenericRecord v;
    private Schema intermediateSchema;
    private Set<String> joinKeys;
    private int valueFieldsCount;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      k = new GenericData.Record(new Schema.Parser().parse(context.getConfiguration().get(KEY_SCHEMA)));
      joinKeys = new HashSet<String>();
      for (String s : context.getConfiguration().get(CONF_KEYS).split(","))
      {
        joinKeys.add(s.trim());
      }
      intermediateSchema = new Schema.Parser().parse(context.getConfiguration().get(INTERMEDIATE_SCHEMA));
      v = new GenericData.Record(intermediateSchema);
      valueFieldsCount = intermediateSchema.getFields().size();
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
        InterruptedException
    {
      GenericRecord datum = key.datum();
      for (String joinKey : joinKeys)
      {
        k.put(joinKey, datum.get(joinKey));
      }
      for (int i = 0; i < valueFieldsCount; i++)
      {
        v.put(i, MISSING_FLAG);
      }
      for (Field f : datum.getSchema().getFields())
      {
        if (!joinKeys.contains(f.name()))
        {
          v.put(f.name(), datum.get(f.pos()));
        }
      }

      context.write(new AvroKey<GenericRecord>(k), new AvroValue<GenericRecord>(v));
    }
  }

  public static class CombineJoin extends
      Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, AvroValue<GenericRecord>>
  {

    private Schema intermediateSchema;
    private GenericRecord v;
    private int valueFieldsCount;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      intermediateSchema = new Schema.Parser().parse(context.getConfiguration().get(INTERMEDIATE_SCHEMA));
      v = new GenericData.Record(intermediateSchema);
      valueFieldsCount = intermediateSchema.getFields().size();
    }

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException,
        InterruptedException
    {
      for (int i = 0; i < valueFieldsCount; i++)
      {
        v.put(i, MISSING_FLAG);
      }
      for (AvroValue<GenericRecord> value : values)
      {
        GenericRecord record = value.datum();
        for (int i = 0; i < valueFieldsCount; i++)
        {
          Object obj = record.get(i);
          if (obj != null && obj.equals(MISSING_FLAG))
          {
            continue;
          }
          v.put(i, obj);
        }
      }
      context.write(key, new AvroValue<GenericRecord>(v));
    }
  }

  public static class ReduceJoin extends
      Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>
  {

    private boolean innerJoin;
    private Schema outputSchema;
    private int valueFieldsCount;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      outputSchema = new Schema.Parser().parse(context.getConfiguration().get(OUTPUT_SCHEMA));
      innerJoin = context.getConfiguration().get(CONF_TYPE, "").equals("inner");
      Schema intermediateSchema = new Schema.Parser().parse(context.getConfiguration().get(INTERMEDIATE_SCHEMA));
      valueFieldsCount = intermediateSchema.getFields().size();
    }

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException,
        InterruptedException
    {
      GenericData.Record res = new GenericData.Record(outputSchema);
      int completeFields = 0;
      for (AvroValue<GenericRecord> value : values)
      {
        GenericRecord record = value.datum();
        for (int i = 0; i < valueFieldsCount; i++)
        {
          Object obj = record.get(i);
          if (obj != null && obj.equals(MISSING_FLAG))
          {
            continue;
          }
          res.put(i, obj);
          completeFields++;
        }
      }
      if (innerJoin && completeFields < valueFieldsCount)
      {
        return;
      }
      for (Field f : key.datum().getSchema().getFields())
      {
        res.put(f.name(), key.datum().get(f.name()));
      }
      context.write(new AvroKey<GenericRecord>(res), NullWritable.get());
    }
  }
}
