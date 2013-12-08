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

package datafu.mr.test;

import java.lang.reflect.Type;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.mr.jobs.IntermediateTypeHelper;

@Test(groups = "pcl")
public class TestIntermediateTypeHelper
{

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonMapperClass()
  {
    IntermediateTypeHelper.getTypes(Object.class, Mapper.class);
  }

  @Test
  public void realMapperTest()
  {
    Type[] types = IntermediateTypeHelper.getTypes(RealMapper.class, Mapper.class);
    Assert.assertNotNull(types);
    Assert.assertEquals(types[0], IntWritable.class);
    Assert.assertEquals(types[1], LongWritable.class);
    Assert.assertEquals(types[2], Text.class);
    Assert.assertEquals(types[3], FloatWritable.class);
  }

  @Test
  public void anonymousMapperTest()
  {
    Mapper<IntWritable, LongWritable, Text, FloatWritable> anonmousMapper =
        new Mapper<IntWritable, LongWritable, Text, FloatWritable>()
        {
        };
    Type[] types = IntermediateTypeHelper.getTypes(anonmousMapper.getClass(), Mapper.class);
    Assert.assertNotNull(types);
    Assert.assertEquals(types[0], IntWritable.class);
    Assert.assertEquals(types[1], LongWritable.class);
    Assert.assertEquals(types[2], Text.class);
    Assert.assertEquals(types[3], FloatWritable.class);
  }

  @Test
  public void realMapperOutputKeyClassTest()
  {
    Assert.assertEquals(IntermediateTypeHelper.getMapperOutputKeyClass(RealMapper.class), Text.class);
  }

  @Test
  public void realMapperOutputValueClassTest()
  {
    Assert.assertEquals(IntermediateTypeHelper.getMapperOutputValueClass(RealMapper.class), FloatWritable.class);
  }

  private static class RealMapper extends Mapper<IntWritable, LongWritable, Text, FloatWritable>
  {
  }
}
