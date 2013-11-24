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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BasicWritableReader<K extends Writable, V extends Writable>
{
  private final Path _inputPath;
  private final FileSystem _fs;
  private final Class<K> _keyClass;
  private final Class<V> _valueClass;

  private SequenceFile.Reader _dataReader;

  public BasicWritableReader(Path inputPath, FileSystem fs, Class<K> keyClass, Class<V> valueClass)
  {
    _inputPath = inputPath;
    _fs = fs;
    _keyClass = keyClass;
    _valueClass = valueClass;
  }

  public void open() throws IOException
  {
    if (_dataReader != null)
    {
      throw new RuntimeException("Already have data reader");
    }

    Path path = new Path(_inputPath, "part-r-00000");

    if (!_fs.exists(path))
    {
      path = new Path(_inputPath, "part-m-00000");
    }

    _dataReader = new SequenceFile.Reader(_fs, path, new Configuration());
  }

  public Map<K, V> readAll() throws IOException
  {
    K key;
    V val;
    try
    {
      key = (K) _keyClass.newInstance();
      val = (V) _valueClass.newInstance();
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }

    Map<K, V> res = new HashMap<K, V>();
    while (_dataReader.next(key, val))
    {
      res.put(key, val);

      try
      {
        key = (K) _keyClass.newInstance();
        val = (V) _valueClass.newInstance();
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
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
