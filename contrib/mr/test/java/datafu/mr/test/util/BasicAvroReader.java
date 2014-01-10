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
package datafu.mr.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BasicAvroReader
{

  private final Path _inputPath;
  private final FileSystem _fs;

  private DataFileStream<Object> _dataReader;

  public BasicAvroReader(Path inputPath, FileSystem fs)
  {
    _inputPath = inputPath;
    _fs = fs;
  }

  public void open() throws IOException
  {
    if (_dataReader != null)
    {
      throw new RuntimeException("Already have data reader");
    }

    Path path = new Path(_inputPath, "part-r-00000.avro");

    if (!_fs.exists(path))
    {
      path = new Path(_inputPath, "part-m-00000.avro");
    }

    FSDataInputStream is = _fs.open(path);
    DatumReader<Object> reader = new GenericDatumReader<Object>();
    _dataReader = new DataFileStream<Object>(is, reader);
  }

  public List<Object> readAll() throws IOException
  {
    List<Object> res = new ArrayList<Object>();
    while (_dataReader.hasNext())
    {
      Object r = _dataReader.next();
      res.add(r);
    }
    return res;
  }

  public void close() throws IOException
  {
    if (_dataReader == null)
    {
      throw new RuntimeException("No data reader");
    }
    _dataReader.close();
    _dataReader = null;
  }
}
