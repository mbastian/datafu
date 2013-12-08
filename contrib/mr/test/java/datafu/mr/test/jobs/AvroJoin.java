package datafu.mr.test.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

/**
 * Inner and outer join MR job using Avro
 * 
 * @author Mathieu Bastian
 */
public class AvroJoin extends AbstractAvroJob
{
  private static String CONF_KEYS = "avrojoin.keys";
  private static String CONF_TYPE = "avrojoin.type";
  private static String UNION_SCHEMA = "avrojoin.union.schema";
  private static String OUTPUT_SCHEMA = "avrojoin.output.schema";
  private static String KEY_SCHEMA = "avrojoin.key.schema";

  @Override
  public void init(Configuration conf)
  {
    Schema[] inputSchemas = null;
    try
    {
      inputSchemas = getInputSchemas();
      conf.set(UNION_SCHEMA, Schema.createUnion(Arrays.asList(inputSchemas)).toString());
    }
    catch (IOException e)
    { 
      throw new RuntimeException(e);
    }

    String[] keys = conf.get(CONF_KEYS).split(",");
    HashMap<String, Schema> keySchemas = new HashMap<String, Schema>();
    List<Field> keyFields = new ArrayList<Field>();
    for (int i = 0; i < keys.length; i++)
    {
      keys[i] = keys[i].trim();

      Schema lastFieldSchema = null;
      for (Schema s : inputSchemas)
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

    List<Field> outputFields = new ArrayList<Schema.Field>();
    for (Schema inputSchema : inputSchemas)
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
  }

  @Override
  protected Schema getMapOutputKeySchema()
  {
    return new Schema.Parser().parse(getConf().get(KEY_SCHEMA));
  }

  @Override
  protected Schema getMapOutputValueSchema()
  {
    return new Schema.Parser().parse(getConf().get(UNION_SCHEMA));
  }

  @Override
  protected Schema getReduceOutputSchema()
  {
    return new Schema.Parser().parse(getConf().get(OUTPUT_SCHEMA));
  }

  public static class MapJoin extends
      Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>>
  {
    private GenericRecord k;
    private String[] joinKeys;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      k = new GenericData.Record(new Schema.Parser().parse(context.getConfiguration().get(KEY_SCHEMA)));
      joinKeys = context.getConfiguration().get(CONF_KEYS).split(",");
      for (int i = 0; i < joinKeys.length; i++)
      {
        joinKeys[i] = joinKeys[i].trim();
      }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
        InterruptedException
    {
      for (String joinKey : joinKeys)
      {
        k.put(joinKey, key.datum().get(joinKey));
      }
      context.write(new AvroKey<GenericRecord>(k), new AvroValue<GenericRecord>(key.datum()));
    }
  }

  public static class ReduceJoin extends
      Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>
  {

    private boolean innerJoin;
    private Schema outputSchema;
    private String[] fields;
    private int inputFiles;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException
    {
      outputSchema = new Schema.Parser().parse(context.getConfiguration().get(OUTPUT_SCHEMA));
      inputFiles = new Schema.Parser().parse(context.getConfiguration().get(UNION_SCHEMA)).getTypes().size();
      fields = new String[outputSchema.getFields().size()];
      innerJoin = context.getConfiguration().get(CONF_TYPE, "").equals("inner");
      int i = 0;
      for (Field f : outputSchema.getFields())
      {
        fields[i++] = f.name();
      }
    }

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException,
        InterruptedException
    {
      GenericData.Record res = new GenericData.Record(outputSchema);
      int valueCount = 0;
      for (AvroValue<GenericRecord> value : values)
      {
        GenericRecord record = value.datum();
        valueCount++;
        for (String f : fields)
        {
          Object obj = record.get(f);
          if (obj != null)
          {
            res.put(f, obj);
          }
        }
      }
      if(innerJoin && valueCount < inputFiles) {
        return;
      }
      context.write(new AvroKey<GenericRecord>(res), NullWritable.get());
    }
  }
}
