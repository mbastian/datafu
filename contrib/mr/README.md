# DataFu: MR

DataFu MR is a lightweight framework for implementing Java MapReduce Hadoop jobs.

## Quick Start Example

The way to use DataFu MR is simply to subclass its `AbstractJob` or `AbstractAvroJob` and implement/override the methods. 

Basic Word Count example:

```java
public class WordCountJob extends AbstractJob
{

  public WordCountJob(String name, Properties props)
  {
    super(name, props);	
  }

  @Override
  public void setupInputFormat(Job job) throws IOException
  {
    job.setInputFormatClass(SequenceFileInputFormat.class);
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException
  {
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException
    {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens())
      {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
  {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException
    {
      int sum = 0;
      for (IntWritable val : values)
      {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}
```

Same word count example, but using `AbstractAvroJob`:

```java
public class AvroWordCountJob extends AbstractAvroJob
{

  public static final Schema OUTPUT_SCHEMA = Schemas.createRecordSchema(AvroWordCountJob.class,
                                                                        "Output",
                                                                        new Field("word",
                                                                                  Schema.create(Type.STRING),
                                                                                  "word",
                                                                                  null),
                                                                        new Field("count",
                                                                                  Schema.create(Type.INT),
                                                                                  "count",
                                                                                  null));

  public AvroWordCountJob(String name, Properties props)
  {
    super(name, props);	
  }

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
```

## Features

* Built-in support for Avro input and output formats
* Though we recommend using Avro, one can use any input/output format class
* Mapper, reducer and intermediate key/value classes are inferred when possible
* Avro schemas are inferred when using POJO objects
* Staged output to avoid deleting the existing file if the job fails
* Estimate the number of reducers needed if not provided
* Supports `#LATEST` suffix in input paths to work with timestamped folders

## Dependencies

DataFu-MR depends on Avro >= 1.7.0 and Hadoop >= 1.0.

For a complete list of dependencies, consult the `ivy.xml` file.

## FAQ

#### How to get started?

Override the `AbstractJob` or `AbstractAvroJob` and define a mapper and reducer class. 

Also, checkout the examples in the `/test/java` folder.

#### How to configure and run a job?

```java
Properties props = new Properties();
props.setProperty("input.path", "/input");
props.setProperty("output.path", "/output");
props.setProperty("mapred.reduce.tasks", 5);

FooJob job = new FooJob("My Foo Job", props);
job.run();
```

##### How to configure settings before the job initializes

Override the `init()` method.

##### How to set configurations just before the job starts

Override the `configure()` method.

##### How to setup the number of reducers

Override the `getNumReducers()` method or setup the `mapred.reduce.tasks` property.

##### How to configure the input location?

Setup the `input.path` property. For multiple inputs, separate the paths with a coma. 

Alternatively the paths may be listed separately. For example, `input.path.first` and `input.path.second` define two separate input paths.

##### How to configure the output location?

Setup the `output.path` property. If the path exists, it will be deleted and replaced with the ouptut.

##### How to configure the temporary location?

Setup the `temp.path` property. The default is `/tmp/`.

##### How to use the distributed cache?

Override the `getDistributedCachePaths` method. It is recommend that you use the [symlink](http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/filecache/DistributedCache.html) feature as it simplifies the way to read the files. 

For instance, use the following code to configure the distributed cache copy from `hdfs://cachefile` to the `suffix` file.

```java
@Override
public List<Path> getDistributedCachePaths()
{
  return Arrays.asList(new Path[] { new Path("/cachefile#suffix")});
}
```

You can also set the `mapred.cache.files` property with a comma-separated list of paths.

##### How to do a map-only job?

Don't override the `getReducerClass()` or override it and return `null`.

##### How to setup multiple outputs?

Configure multiple outputs by using [MultipleOutputs](org.apache.hadoop.mapreduce.lib.output.MultipleOutputs) from the MapReduce API.

For instance, use the following code to configure the output to two different files `foo` and `bar`.

```java
@Override
public void setupOutputFormat(Job job) throws IOException
{
  MultipleOutputs.addNamedOutput(job, "foo", SequenceFileOutputFormat.class, IntWritable.class, Text.class);
  MultipleOutputs.addNamedOutput(job, "bar", SequenceFileOutputFormat.class, IntWritable.class, Text.class);
}
```

Then, use the `MultipleOutputs.write()` method to configure which file output to write to.

##### What is the #LATEST suffix for?

If an input path ends with #LATEST (e.g. `/data/events/#LATEST`), the system will browse the folder and pick the first folder by lexicographic order. For instance, if `/data/events` contains two folders `/data/events/2013-01-01` and `/data/events/2014-01-01` it will replace `#LATEST` by `2014-01-01`.

Set the `use.latest.expansion` property to `true` to enable this feature. The default value is `false`.

##### How to use a custom input or output formats?

In the case of an job extending `AbstractJob`, the `setupInputFormat()` and `setupOutputFormat()` methods have to be implemented so it's up to you which format to use. In the case of an `AbstractAvroJob`, the default input and output format are Avro but can be overridden. For instance, to use Avro as input but customize the output format simply override the `setupOutputFormat()` method.

##### How are the mapper and reducer classes configured?

If the job class contains Mapper and Reducer inner classes they are automatically set as mapper and reducer for the job. The automatic setup doesn't work if the class contains multiple inner classes extending `Mapper` or `Reducer`. In that case, you can implement the `getMapperClass()` and `getReducerClass()` methods. 

It's also possible to define values for the `mapreduce.map.class` and `mapreduce.reduce.class` properties.

##### How to use a combiner?

Override the `getCombinerClass()` method. If you prefer to define this as a job property, configure the `mapreduce.combine.class` value.

##### How to use a partitioner?

Override the `getPartitionerClass()` method. If you prefer to define this as a job property, configure the `mapreduce.partitioner.class` value.

##### How to use a grouping comparator?

Override the `getGroupingComparator()` method. If you prefer to define this as a job property, configure the `mapred.output.value.groupfn.class` value.

##### How to use a sort comparator?

Override the `getSortComparator()` method. If you prefer to define this as a job property, configure the `mapred.output.key.comparator.class` value.

##### How to provide the Avro output schema?

Override the `getOutputSchema()` method. If the output is a Java primitive or a POJO object, the schema will be automatically inferred so it's not necessary to implement this method.

In the case of a map-only job, implement the `getOutputSchema()` to define the mapper output schema.

If you prefer to define this as a job property, configure the `output.schema` value.

##### Can an Avro job use Writable as intermediate types?

Yes, that is supported. Take a look at the `BasicAvroIntermediateWritableJob` job test example.

##### Can an Avro job output a Java POJO object?

Yes, this is supported both as an output of the mapper and the reducer. In most cases, you don't have to provide a schema as the system will automatically infer the schema by introspection into the class.

##### Are the input files accessible in the `init()` method?

Yes, one can always call the `getInputPaths()` method. In the case of an Avro job, one can also call `getInputSchemas()` to obtain the schema of each input path.

##### Can MapReduce jobs be implemented in Scala?

Yes, this is possible. Here is an example of an Avro job:

```scala
object TestScala extends AbstractAvroJob {

  class TestScalaMapper extends Mapper[AvroKey[GenericRecord], NullWritable, Text, IntWritable] {

    override def map(key: AvroKey[GenericRecord], value: NullWritable, context: Mapper[AvroKey[GenericRecord], NullWritable, Text, IntWritable]#Context) {
      
    }
  }

  class TestScalaReducer extends Reducer[Text, IntWritable, AvroKey[GenericRecord], NullWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, AvroKey[GenericRecord], NullWritable]#Context) {
      
    }
  }
}

class TestScala(name: String, props: Properties) extends AbstractAvroJob(name, props) {
	
    outputSchema = SchemaBuilder.record("data").fields().
	    name("word").`type`().stringType().noDefault().
	    name("count").`type`().longType().noDefault().endRecord()
}
```

## Design notes

The framework is based on the new Hadoop MapReduce API (org.apache.hadoop.mapreduce). It's a thin layer on top of the Hadoop API yet it simplifies key concepts and reduce the amount of boilerplate and configuration code. 

## Developers

### How to build

	ant
		
### How to test

	ant test

## Contribute

The source code is available under the Apache 2.0 license. Contributions are welcome.