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
