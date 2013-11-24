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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("rawtypes")
public class BasicWritableWriter<K extends Writable, V extends Writable>
{
  private final Path _outputPath;
  private final FileSystem _fs;
  private final Class<K> _keyClass;
  private final Class<V> _valueClass;

  private SequenceFile.Writer _dataWriter;

  public BasicWritableWriter(Path outputPath, FileSystem fs, Class<K> keyClass, Class<V> valueClass)
  {
    _outputPath = outputPath;
    _fs = fs;
    _keyClass = keyClass;
    _valueClass = valueClass;
  }

  public void open() throws IOException
  {
    if (_dataWriter != null)
    {
      throw new RuntimeException("Already have data writer");
    }

    Path path = _outputPath;

    _dataWriter =
        SequenceFile.createWriter(_fs, new Configuration(), new Path(path, "part-00000"), _keyClass, _valueClass);
  }

  public void append(K key, V value) throws IOException
  {
    if (_dataWriter == null)
    {
      throw new RuntimeException("No data writer");
    }
    if (!key.getClass().equals(_keyClass))
    {
      throw new RuntimeException("The key class doesn't match, actual=" + key.getClass().getSimpleName()
          + ", expected=" + _keyClass.getClass().getSimpleName());
    }
    if (!value.getClass().equals(_valueClass))
    {
      throw new RuntimeException("The value class doesn't match, actual=" + value.getClass().getSimpleName()
          + ", expected=" + _valueClass.getClass().getSimpleName());
    }
    _dataWriter.append(key, value);
  }

  public void close() throws IOException
  {
    if (_dataWriter == null)
    {
      throw new RuntimeException("No data writer");
    }
    _dataWriter.close();
    _dataWriter = null;
  }
}
