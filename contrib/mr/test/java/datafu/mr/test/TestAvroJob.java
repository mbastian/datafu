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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.avro.Schemas;
import datafu.mr.fs.PathUtils;
import datafu.mr.jobs.AbstractAvroJob;
import datafu.mr.test.jobs.BasicAvroIntermediateObjectJob;
import datafu.mr.test.jobs.BasicAvroIntermediateWritableJob;
import datafu.mr.test.jobs.BasicAvroJob;
import datafu.mr.test.jobs.BasicAvroMapOnlyJob;
import datafu.mr.test.jobs.BasicAvroMapOnlyOutputObjectJob;
import datafu.mr.test.jobs.BasicAvroMultipleInputsJob;
import datafu.mr.test.jobs.BasicAvroMultipleOutputsJob;
import datafu.mr.test.jobs.BasicAvroOutputObjectJob;
import datafu.mr.test.jobs.BasicAvroWordCountJob;
import datafu.mr.test.util.BasicAvroReader;
import datafu.mr.test.util.BasicAvroWriter;
import datafu.mr.test.util.BasicWritableReader;

@Test(groups = "pcl")
public class TestAvroJob extends TestBase
{
  private Logger _log = Logger.getLogger(TestAvroJob.class);

  private Path _inputPath = new Path("/input");
  private Path _outputPath = new Path("/output");

  private static final Schema EVENT_SCHEMA;
  private static final Schema KEY_A_SCHEMA;
  private static final Schema KEY_B_SCHEMA;

  static
  {
    EVENT_SCHEMA =
        Schemas.createRecordSchema(TestAvroJob.class, "Event", new Field("id", Schema.create(Type.LONG), "ID", null));
    KEY_A_SCHEMA =
        Schemas.createRecordSchema(TestAvroJob.class,
                                   "KeyA",
                                   new Field("key", Schema.create(Type.LONG), "key", null),
                                   new Field("value_a", Schema.create(Type.LONG), "key", null));
    KEY_B_SCHEMA =
        Schemas.createRecordSchema(TestAvroJob.class,
                                   "KeyB",
                                   new Field("key", Schema.create(Type.LONG), "key", null),
                                   new Field("value_b", Schema.create(Type.LONG), "key", null));
  }

  public TestAvroJob() throws IOException
  {
    super();
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    super.afterClass();
  }

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException
  {
    _log.info("*** Running " + method.getName());

    _log.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPath, true);
    getFileSystem().delete(_outputPath, true);
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().mkdirs(_outputPath);
  }

  @Test
  public void basicAvroJobTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicJob();
    configureAndRunJob(new BasicAvroJob(), "BasicAvroJob", _inputPath, _outputPath);
    checkBasicJob();
  }

  @Test
  public void basicAvroJobIntermediateObjectTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicJob();
    configureAndRunJob(new BasicAvroIntermediateObjectJob(),
                       "BasicAvroJobIntermediateObjectJob",
                       _inputPath,
                       _outputPath);
    checkBasicJob();
  }

  @Test
  public void basicAvroJobOutputObjectTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicJob();
    configureAndRunJob(new BasicAvroOutputObjectJob(), "BasicAvroOutputObjectJob", _inputPath, _outputPath);
    checkBasicJob();
  }

  @Test
  public void basicAvroJobIntermediateWritableTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicJob();
    configureAndRunJob(new BasicAvroIntermediateWritableJob(),
                       "BasicAvroJobIntermediateWritableJob",
                       _inputPath,
                       _outputPath);
    checkBasicJob();
  }

  @Test
  public void basicAvroMultipleInputsJobTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    Path multipleInput = new Path(_inputPath.toString() + "/A," + _inputPath.toString() + "/B");
    initBasicMultipleInputsJob();
    configureAndRunJob(new BasicAvroMultipleInputsJob(), "BasicAvroMultipleInputsJob", multipleInput, _outputPath);
    checkBasicMultipleInputsJob();
  }

  @Test
  public void basicAvroMapOnlyJob() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicMapOnlyJob();
    configureAndRunJob(new BasicAvroMapOnlyJob(), "BasicAvroMapOnlyJob", _inputPath, _outputPath);
    checkBasicMapOnlyJob();
  }

  @Test
  public void basicAvroMapOnlyOutputObjectJob() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicMapOnlyJob();
    configureAndRunJob(new BasicAvroMapOnlyOutputObjectJob(),
                       "BasicAvroMapOnlyOutputObjectJob",
                       _inputPath,
                       _outputPath);
    checkBasicMapOnlyJob();
  }

  @Test
  public void basicAvroWordCountJob() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initPrimitiveJob();
    configureAndRunJob(new BasicAvroWordCountJob(), "BasicAvroWordCountJob", _inputPath, _outputPath);
    checkWordCountJob();
  }

  @Test
  public void basicAvroMultipleOutputsJobTest() throws IOException,
      InterruptedException,
      ClassNotFoundException
  {
    initBasicJob();
    configureAndRunJob(new BasicAvroMultipleOutputsJob(), "BasicAvroMultipleOutputsJob", _inputPath, _outputPath);
    checkBasicMultipleOutputsJob();
  }

  // UTILITIES

  private void initBasicJob() throws IOException
  {
    BasicAvroWriter writer = new BasicAvroWriter(_inputPath, EVENT_SCHEMA, getFileSystem());
    writer.open();
    storeIds(writer, 1, 1, 1, 1, 1);
    storeIds(writer, 2, 2, 2);
    storeIds(writer, 3, 3, 3, 3);
    storeIds(writer, 4, 4, 4);
    storeIds(writer, 5);
    writer.close();
  }

  private void checkBasicJob() throws IOException
  {
    checkOutputFolderCount(1);

    HashMap<Long, Long> counts = loadOutputCounts();
    checkSize(counts, 5);
    checkIdCount(counts, 1, 5);
    checkIdCount(counts, 2, 3);
    checkIdCount(counts, 3, 4);
    checkIdCount(counts, 4, 3);
    checkIdCount(counts, 5, 1);
  }

  private void checkBasicMultipleOutputsJob() throws IOException
  {
	checkOutputFolderCount(2);
	  
    HashMap<Long, Long> countsEven = loadMultipleOutputsCounts("even");

    checkSize(countsEven, 2);
    checkIdCount(countsEven, 2, 3);
    checkIdCount(countsEven, 4, 3);

    HashMap<Long, Long> countsOdd = loadMultipleOutputsCounts("odd");

    checkSize(countsOdd, 3);
    checkIdCount(countsOdd, 1, 5);
    checkIdCount(countsOdd, 3, 4);
    checkIdCount(countsOdd, 5, 1);
  }

  private void initBasicMultipleInputsJob() throws IOException
  {
    BasicAvroWriter writerA = new BasicAvroWriter(new Path(_inputPath, "A"), KEY_A_SCHEMA, getFileSystem());
    writerA.open();
    storeKeysA(writerA, 1, 2, 3, 4);
    writerA.close();

    BasicAvroWriter writerB = new BasicAvroWriter(new Path(_inputPath, "B"), KEY_B_SCHEMA, getFileSystem());
    writerB.open();
    storeKeysB(writerB, 1, 2, 3, 5);
    writerB.close();
  }

  private void checkBasicMultipleInputsJob() throws IOException
  {
    checkOutputFolderCount(1);

    HashMap<Long, Long> counts = loadOutputCounts();
    checkSize(counts, 5);
    checkIdCount(counts, 1, 2);
    checkIdCount(counts, 2, 2);
    checkIdCount(counts, 3, 2);
    checkIdCount(counts, 4, 1);
    checkIdCount(counts, 5, 1);
  }

  private void initBasicMapOnlyJob() throws IOException
  {
    BasicAvroWriter writer = new BasicAvroWriter(_inputPath, EVENT_SCHEMA, getFileSystem());
    writer.open();
    storeIds(writer, 1, 2, 3);
    writer.close();
  }

  private void checkBasicMapOnlyJob() throws IOException
  {
    checkOutputFolderCount(1);

    HashMap<Long, Long> counts = loadOutputCounts();
    checkSize(counts, 3);
    checkIdCount(counts, 1, 1);
    checkIdCount(counts, 2, 1);
    checkIdCount(counts, 3, 1);
  }

  private void initPrimitiveJob() throws IOException
  {
    BasicAvroWriter writer = new BasicAvroWriter(_inputPath, Schema.create(Type.STRING), getFileSystem());
    writer.open();
    writer.append("foo bar");
    writer.append("foo");
    writer.close();
  }

  private void checkWordCountJob() throws IOException
  {
    checkOutputFolderCount(1);

    HashMap<String, Integer> counts = loadOutputWordCounts();
    checkSize(counts, 2);
    checkWordCount(counts, "foo", 2);
    checkWordCount(counts, "bar", 1);
  }

  private void checkSize(HashMap<?, ?> counts, int expectedSize)
  {
    Assert.assertEquals(counts.size(), expectedSize);
  }

  private void checkIdCount(HashMap<Long, Long> counts, long id, long count)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).longValue(), count);
  }

  private void checkWordCount(HashMap<String, Integer> counts, String id, int count)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).intValue(), count);
  }

  private void checkOutputFolderCount(int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(), expectedCount, "Found: " + listOutputFolders());
  }

  private void checkMultipleOutputFolderCount(int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(), expectedCount, "Found: " + listOutputFolders());
  }

  private int countOutputFolders() throws IOException
  {
    return countOutputFolders(_outputPath);
  }

  private int countOutputFolders(Path path) throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.listStatus(path, PathUtils.nonHiddenPathFilter).length;
  }

  private String listOutputFolders() throws IOException
  {
    StringBuilder sb = new StringBuilder();
    for (FileStatus stat : getFileSystem().listStatus(_outputPath, PathUtils.nonHiddenPathFilter))
    {
      sb.append(stat.getPath().getName());
      sb.append(",");
    }
    return sb.toString();
  }

  private void storeIds(BasicAvroWriter writer, long... ids) throws IOException
  {
    GenericRecord record = new GenericData.Record(EVENT_SCHEMA);
    for (long id : ids)
    {
      record.put("id", id);
      writer.append(record);
    }
  }

  private void storeKeysA(BasicAvroWriter writer, long... ids) throws IOException
  {
    GenericRecord record = new GenericData.Record(KEY_A_SCHEMA);
    for (long id : ids)
    {
      record.put("key", id);
      record.put("value_a", 1l);
      writer.append(record);
    }
  }

  private void storeKeysB(BasicAvroWriter writer, long... ids) throws IOException
  {
    GenericRecord record = new GenericData.Record(KEY_B_SCHEMA);
    for (long id : ids)
    {
      record.put("key", id);
      record.put("value_b", 1l);
      writer.append(record);
    }
  }

  private HashMap<Long, Long> loadOutputCounts() throws IOException
  {
    HashMap<Long, Long> counts = new HashMap<Long, Long>();

    BasicAvroReader reader = new BasicAvroReader(_outputPath, getFileSystem());
    reader.open();
    for (Object o : reader.readAll())
    {
      GenericRecord r = (GenericRecord) o;
      Long memberId = (Long) r.get("key");
      Long count = (Long) r.get("count");
      Assert.assertFalse(counts.containsKey(memberId));
      counts.put(memberId, count);
    }
    reader.close();
    return counts;
  }

  private HashMap<String, Integer> loadOutputWordCounts() throws IOException
  {
    HashMap<String, Integer> counts = new HashMap<String, Integer>();

    BasicAvroReader reader = new BasicAvroReader(_outputPath, getFileSystem());
    reader.open();
    for (Object o : reader.readAll())
    {
      GenericRecord r = (GenericRecord) o;
      String word = ((CharSequence) r.get("word")).toString();
      Integer count = (Integer) r.get("count");
      Assert.assertFalse(counts.containsKey(word));
      counts.put(word, count);
    }
    reader.close();
    return counts;
  }

  private HashMap<Long, Long> loadMultipleOutputsCounts(String folder) throws IOException
  {
    HashMap<Long, Long> counts = new HashMap<Long, Long>();

    BasicAvroReader reader = new BasicAvroReader(new Path(_outputPath.toString() + "/" + folder), getFileSystem());
    reader.open();
    for (Object o : reader.readAll())
    {
      GenericRecord r = (GenericRecord) o;
      Long memberId = (Long) r.get("key");
      Long count = (Long) r.get("count");
      Assert.assertFalse(counts.containsKey(memberId));
      counts.put(memberId, count);
    }
    reader.close();
    return counts;
  }

  private void configureAndRunJob(AbstractAvroJob job, String name, Path inputPath, Path outputPath) throws IOException,
      ClassNotFoundException,
      InterruptedException
  {
    Properties _props = newTestProperties();
    _props.setProperty("input.path", inputPath.toString());
    _props.setProperty("output.path", outputPath.toString());
    job.setProperties(_props);
    job.setName(name);
    job.run();
  }
}
