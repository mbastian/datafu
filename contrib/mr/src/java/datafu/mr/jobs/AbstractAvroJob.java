package datafu.mr.jobs;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import datafu.mr.avro.CombinedAvroKeyInputFormat;

public abstract class AbstractAvroJob extends AbstractJob
{

  private final Logger _log = Logger.getLogger(AbstractAvroJob.class);

  private boolean _combineInputs;

  public AbstractAvroJob()
  {
    setConf(new Configuration());
  }

  public AbstractAvroJob(String name, Properties props) throws IOException
  {
    super(name, props);

    if (props.containsKey("combine.inputs"))
    {
      setCombineInputs(Boolean.parseBoolean(props.getProperty("combine.inputs")));
    }
  }

  protected abstract Schema getReduceOutputSchema();

  protected Schema getMapOutputKeySchema()
  {
    return null;
  }

  protected Schema getMapOutputValueSchema()
  {
    return null;
  }

  @Override
  public void setupInputFormat(Job job) throws IOException
  {
    if (_combineInputs)
    {
      job.setInputFormatClass(CombinedAvroKeyInputFormat.class);
    }
    else
    {
      job.setInputFormatClass(AvroKeyInputFormat.class);
    }

    // Schema inputSchema = PathUtils.getSchemaFromPath(getFileSystem(),
    // getInputPaths().get(0));
    // AvroJob.setInputKeySchema(job, inputSchema);
  }

  @Override
  public void setupIntermediateFormat(Job job) throws IOException
  {
    if (AvroKey.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputKeyClass(getMapperClass())))
    {
      if (getMapOutputKeySchema() != null)
      {
        AvroJob.setMapOutputKeySchema(job, getMapOutputKeySchema());
        _log.info(String.format("Set map output key schema: %s", getMapOutputKeySchema().toString()));
      }
      else
      {
        // Infer schema
      }
    }
    if (AvroValue.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputValueClass(getMapperClass())))
    {
      if (getMapOutputValueSchema() != null)
      {
        AvroJob.setMapOutputValueSchema(job, getMapOutputValueSchema());
        _log.info(String.format("Set map output value schema: %s", getMapOutputValueSchema().toString()));
      }
      else
      {
        // Infer schema
      }
    }
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException
  {
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    AvroJob.setOutputKeySchema(job, getReduceOutputSchema());
    _log.info(String.format("Set output key schema: %s", getReduceOutputSchema().toString()));
  }

  /**
   * Gets whether inputs should be combined.
   * 
   * @return true if inputs are to be combined
   */
  public boolean getCombineInputs()
  {
    return _combineInputs;
  }

  /**
   * Sets whether inputs should be combined.
   * 
   * @param combineInputs
   *          true to combine inputs
   */
  public void setCombineInputs(boolean combineInputs)
  {
    _combineInputs = combineInputs;
  }
}
