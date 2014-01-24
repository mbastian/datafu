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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import datafu.mr.fs.PathUtils;
import datafu.mr.util.CurrentExpansionFunction;

/**
 * Utility job to move folders and delete old versions.
 * <p>
 * The job works with a given <em>export.spec</em> JSON configuration string which specifies the
 * source, the destination and the number of folders to keep. When removing old folders from the
 * destination, it sorts folders lexicographically based on their name and keep at most the number
 * wanted.
 * </p>
 * <p>
 * This is a configuration example for <em>export.spec</em>:
 * </p>
 * <p>
 * <code>[{"source":"/input", "dest":"/output/#CURRENT","keep":3}]</code>
 * </p>
 * The <em>#CURRENT</em> suffix will be replaced by the current date using the
 * <em>yyyy-MM-dd-HH-mm</em> format.
 * 
 * @author Matthew Hayes
 */
public class ExportJob extends AbstractJob
{
  private final Logger _log = Logger.getLogger(AbstractJob.class);

  private String _exportSpec;

  public ExportJob(String name, Properties props)
  {
    super(name, props);

    if (props.containsKey("export.spec"))
    {
      setExportSpec(props.getProperty("export.spec"));
    }
    else
    {
      throw new IllegalArgumentException("no export spec [export.spec] specified.");
    }
  }

  @Override
  public void run()
  {
    Configuration config = getConf();

    try
    {
      JSONArray exportSpec = new JSONArray(_exportSpec);

      for (int i = 0; i < exportSpec.length(); i++)
      {
        JSONObject obj = (JSONObject) exportSpec.get(i);
        String source = (String) obj.get("source");
        String dest = (String) obj.get("dest");
        int keep = 0;
        if (obj.has("keep"))
        {
          keep = (Integer) obj.get("keep");
        }

        CurrentExpansionFunction expFunction = new CurrentExpansionFunction(_log);
        dest = expFunction.apply(dest);

        Path sourcePath = new Path(source);
        Path destPath = new Path(dest);

        FileSystem fs = sourcePath.getFileSystem(config);

        fs.mkdirs(destPath);

        _log.info(String.format("Deleting data at old path[%s]", destPath));
        fs.delete(destPath, true);

        _log.info(String.format("Moving from [%s] to [%s]", sourcePath, destPath));
        fs.rename(sourcePath, destPath);

        PathUtils.keepLatestDateTimedPaths(fs, destPath.getParent(), keep);
      }

    }
    catch (JSONException e)
    {
      throw new RuntimeException("Exception when parsing the JSON spec", e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  public String getExportSpec()
  {
    return _exportSpec;
  }

  public void setExportSpec(String exportSpec)
  {
    this._exportSpec = exportSpec;
  }
}
