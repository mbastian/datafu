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
package datafu.mr.jobs;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

  @SuppressWarnings("rawtypes")
  @Override
  public void setupIntermediateFormat(Job job) throws IOException
  {
    Class<? extends Mapper> mapperClass = DiscoveryHelper.getMapperClass(this);

    if (AvroKey.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputKeyClass(mapperClass)))
    {
      if (getMapOutputKeySchema() != null)
      {
        AvroJob.setMapOutputKeySchema(job, getMapOutputKeySchema());
        _log.info(String.format("Set map output key schema: %s", getMapOutputKeySchema().toString()));
      }
      else
      {
        // Infer schema
        ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[2];
        Class<?> keyClass = (Class<?>) type.getActualTypeArguments()[0];
        if (keyClass.equals(GenericData.Record.class))
        {
          _log.warn("Can't infer schema of GenericData.Record");
        }
        Schema schema = ReflectData.get().getSchema(keyClass);
        AvroJob.setMapOutputKeySchema(job, schema);
        _log.info(String.format("Infer map output value schema from %s class: %s",
                                keyClass.getName(),
                                schema.toString()));
      }
    }
    if (AvroValue.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputValueClass(mapperClass)))
    {
      if (getMapOutputValueSchema() != null)
      {
        AvroJob.setMapOutputValueSchema(job, getMapOutputValueSchema());
        _log.info(String.format("Set map output value schema: %s", getMapOutputValueSchema().toString()));
      }
      else
      {
        // Infer schema
        ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[3];
        Class<?> valueClass = (Class<?>) type.getActualTypeArguments()[0];
        if (valueClass.equals(GenericData.Record.class))
        {
          _log.warn("Can't infer schema of GenericData.Record");
        }
        Schema schema = ReflectData.get().getSchema(valueClass);
        AvroJob.setMapOutputValueSchema(job, schema);
        _log.info(String.format("Infer map output value schema from %s class: %s",
                                valueClass.getName(),
                                schema.toString()));
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
