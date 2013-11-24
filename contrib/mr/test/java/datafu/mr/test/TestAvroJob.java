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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
import datafu.mr.test.jobs.BasicAvroJob;
import datafu.mr.test.jobs.BasicAvroJobIntermediateWritable;
import datafu.mr.test.util.BasicAvroWriter;

@Test(groups = "pcl")
public class TestAvroJob extends TestBase {
	private Logger _log = Logger.getLogger(TestAvroJob.class);

	private Path _inputPath = new Path("/input");
	private Path _outputPath = new Path("/output");

	private static final Schema EVENT_SCHEMA;

	private Properties _props;
	private GenericRecord _record;
	private BasicAvroWriter _writer;

	static {
		EVENT_SCHEMA = Schemas.createRecordSchema(TestAvroJob.class, "Event",
				new Field("id", Schema.create(Type.LONG), "ID", null));
	}

	public TestAvroJob() throws IOException {
		super();
	}

	@BeforeClass
	public void beforeClass() throws Exception {
		super.beforeClass();
	}

	@AfterClass
	public void afterClass() throws Exception {
		super.afterClass();
	}

	@BeforeMethod
	public void beforeMethod(Method method) throws IOException {
		_log.info("*** Running " + method.getName());

		_log.info("*** Cleaning input and output paths");
		getFileSystem().delete(_inputPath, true);
		getFileSystem().delete(_outputPath, true);
		getFileSystem().mkdirs(_inputPath);
		getFileSystem().mkdirs(_outputPath);

		_record = new GenericData.Record(EVENT_SCHEMA);

		_writer = new BasicAvroWriter(_inputPath, EVENT_SCHEMA, getFileSystem());
	}

	@Test
	public void basicAvroJobTest() throws IOException, InterruptedException,
			ClassNotFoundException {
		initBasicJob();
		configureAndRunJob(new BasicAvroJob(), "BasicAvroJob", _inputPath, _outputPath);
		checkBasicJob();
	}

	@Test
	public void basicAvroJobIntermediateWritableTest() throws IOException,
			InterruptedException, ClassNotFoundException {
		initBasicJob();
		configureAndRunJob(new BasicAvroJobIntermediateWritable(), "BasicAvroJobIntermediateWritable", _inputPath, _outputPath);
		checkBasicJob();
	}

	private void initBasicJob() throws IOException {
		open();
		storeIds(1, 1, 1, 1, 1);
		storeIds(2, 2, 2);
		storeIds(3, 3, 3, 3);
		storeIds(4, 4, 4);
		storeIds(5);
		close();
	}

	private void checkBasicJob() throws IOException {
		checkOutputFolderCount(1);

		HashMap<Long, Long> counts = loadOutputCounts();
		checkSize(counts, 5);
		checkIdCount(counts, 1, 5);
		checkIdCount(counts, 2, 3);
		checkIdCount(counts, 3, 4);
		checkIdCount(counts, 4, 3);
		checkIdCount(counts, 5, 1);
	}

	private void checkSize(HashMap<Long, Long> counts, int expectedSize) {
		Assert.assertEquals(counts.size(), expectedSize);
	}

	private void checkIdCount(HashMap<Long, Long> counts, long id, long count) {
		Assert.assertTrue(counts.containsKey(id));
		Assert.assertEquals(counts.get(id).longValue(), count);
	}

	private void checkOutputFolderCount(int expectedCount) throws IOException {
		Assert.assertEquals(countOutputFolders(), expectedCount, "Found: "
				+ listOutputFolders());
	}

	private int countOutputFolders() throws IOException {
		FileSystem fs = getFileSystem();
		return fs.listStatus(_outputPath, PathUtils.nonHiddenPathFilter).length;
	}

	private String listOutputFolders() throws IOException {
		StringBuilder sb = new StringBuilder();
		for (FileStatus stat : getFileSystem().listStatus(_outputPath,
				PathUtils.nonHiddenPathFilter)) {
			sb.append(stat.getPath().getName());
			sb.append(",");
		}
		return sb.toString();
	}

	private void storeIds(long... ids) throws IOException {
		for (long id : ids) {
			storeId(id);
		}
	}

	private void storeId(long id) throws IOException {
		_record.put("id", id);
		_writer.append(_record);
	}

	private void open() throws IOException {
		_writer.open();
	}

	private void close() throws IOException {
		_writer.close();
	}

	private HashMap<Long, Long> loadOutputCounts() throws IOException {
		HashMap<Long, Long> counts = new HashMap<Long, Long>();
		FileSystem fs = getFileSystem();
		Assert.assertTrue(fs.exists(_outputPath));
		
		for(FileStatus s : fs.listStatus(_outputPath)) {
			System.out.println("AVROREAD "+s.getPath().toString());
		}

		for (FileStatus stat : fs.globStatus(new Path(_outputPath.toString()
				+ "/*.avro"))) {
			_log.info(String.format("found: %s (%d bytes)", stat.getPath(),
					stat.getLen()));
			FSDataInputStream is = fs.open(stat.getPath());
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
			DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(
					is, reader);

			try {
				while (dataFileStream.hasNext()) {
					GenericRecord r = dataFileStream.next();
					Long memberId = (Long) r.get("id");
					Long count = (Long) r.get("count");
					Assert.assertFalse(counts.containsKey(memberId));
					counts.put(memberId, count);
				}
			} finally {
				dataFileStream.close();
			}
		}
		return counts;
	}
	
	private void configureAndRunJob(AbstractAvroJob job, String name,
			Path inputPath, Path outputPath) throws IOException,
			ClassNotFoundException, InterruptedException {
		Properties _props = newTestProperties();
		_props.setProperty("input.path", inputPath.toString());
		_props.setProperty("output.path", outputPath.toString());
		job.setProperties(_props);
		job.setName(name);
		job.run();
	}
}
