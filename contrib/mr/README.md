# DataFu: MR

DataFu MR is a lightweight framework for implementing Java MapReduce Hadoop jobs.

## Quick Start Example

The way to use DataFu MR is simply to subclass `AbstractJob` or `AbstractAvroJob` and implement/override the methods. 

Basic Word Count example:

```java
public class WordCountJob extends AbstractJob
{

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

## Features

* Built-in support for Avro input and output formats
* Though we recommend using Avro, one can use any input/output format class
* Mapper, reducer and intermediate key/value classes are inferred when possible
* Avro schemas are inferred when using POJO objects
* Staged output to avoid deleting the existing file if the job fails
* Estimate the number of reducers needed if not provided
* Supports `#LATEST` suffix in input paths to work with timestamped folders

## FAQ

#### How to get started?

Override the `AbstractJob` or `AbstractAvroJob` and define a mapper and reducer class. 

Also, checkout the examples in the `/test/java` folder.

#### How to configure and run a job?

```java
Properties props = new Properties();
props.setProperty("input.path", "/input");
props.setProperty("output.path", "/output");

FooJob job = new FooJob();
job.setProperties(props);
job.run();
```

##### How to configure settings before the job initializes

Override the `init()` method.

##### How to set configurations just before the job starts

Override the `configure()` method.

##### How to setup the number of reducers

Override the `getNumReducers()` method or setup the `num.reducers` or `mapred.reduce.tasks` settings.

#### How to configure the input location?

Setup the `input.path` parameter. For multiple inputs, separate the paths with a coma.

##### How to configure the output location?

Setup the `output.path` parameter. If the path exists, it will be deleted and replaced with the ouptut.

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

## Design notes

The framework is based on the new Hadoop MapReduce API (org.apache.hadoop.mapreduce). It's a thin layer on top of the Hadoop API yet it simplifies key concepts and reduce the amount of boilerplate and configuration code. 