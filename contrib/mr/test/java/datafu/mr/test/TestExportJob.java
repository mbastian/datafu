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
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.fs.PathUtils;
import datafu.mr.jobs.ExportJob;

public class TestExportJob extends TestBase
{

  private final Logger _log = Logger.getLogger(TestExportJob.class);
  private final Path _inputPath = new Path("/input");
  private final Path _outputPath = new Path("/output");

  public TestExportJob() throws IOException
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
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().delete(_outputPath, true);
    getFileSystem().mkdirs(_outputPath);
  }

  @Test
  public void simpleTest() throws IOException
  {
    Path input = new Path(_inputPath, "FOO");
    writeFile(input, "");
    Path outputA = new Path(_outputPath, "201401011200");
    writeFile(outputA, "");
    Path outputB = new Path(_outputPath, "201401011201");
    writeFile(outputB, "");
    Path outputC = new Path(_outputPath, "201401011202");
    writeFile(outputC, "");

    Properties _props = newTestProperties();
    _props.setProperty("export.spec", "[{\"source\":\"" + input + "\",\"dest\":\"" + _outputPath
        + "/#CURRENT\",\"keep\":3}]");
    ExportJob exportJob = new ExportJob("ExportJob", _props);
    exportJob.run();

    Assert.assertFalse(getFileSystem().exists(input));
    Assert.assertTrue(getFileSystem().exists(_outputPath));
    Assert.assertEquals(countOutputFolders(), 3);
    Assert.assertTrue(getFileSystem().exists(outputC));
    Assert.assertTrue(getFileSystem().exists(outputB));
    Assert.assertFalse(getFileSystem().exists(outputA));
  }

  // UTILITY

  private void writeFile(Path path, String content) throws IOException
  {
    FileSystem _fs = getFileSystem();
    _fs.mkdirs(path);

    _log.info("*** Write file in " + path);
    Path filePath = new Path(path, "part-00000");
    FSDataOutputStream fin = _fs.create(filePath);
    fin.writeUTF(content);
    fin.close();

    Assert.assertTrue(_fs.exists(filePath));
  }

  private int countOutputFolders() throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.listStatus(_outputPath, PathUtils.nonHiddenPathFilter).length;
  }
}
