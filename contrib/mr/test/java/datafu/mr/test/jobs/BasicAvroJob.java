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

package datafu.mr.test.jobs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.avro.Schemas;
import datafu.mr.jobs.AbstractAvroJob;

public class BasicAvroJob extends AbstractAvroJob {
	public static final Schema KEY_SCHEMA;
	public static final Schema VALUE_SCHEMA;
	public static final Schema OUTPUT_SCHEMA;

	static {
		KEY_SCHEMA = Schemas.createRecordSchema(BasicAvroJob.class, "Key",
				new Field("identifier", Schema.create(Type.LONG), "identifier", null));
		VALUE_SCHEMA = Schemas.createRecordSchema(BasicAvroJob.class, "Value",
				new Field("count", Schema.create(Type.LONG), "count", null));
		OUTPUT_SCHEMA = Schemas.createRecordSchema(BasicAvroJob.class,
				"Output",
				new Field("identifier", Schema.create(Type.LONG), "identifier", null),
				new Field("count", Schema.create(Type.LONG), "count", null));
	}

	@Override
	protected Schema getMapOutputKeySchema() {
		return KEY_SCHEMA;
	}

	@Override
	protected Schema getMapOutputValueSchema() {
		return VALUE_SCHEMA;
	}

	@Override
	protected Schema getReduceOutputSchema() {
		return OUTPUT_SCHEMA;
	}

	@Override
	public Class<? extends Mapper> getMapperClass() {
		return TheMapper.class;
	}

	@Override
	public Class<? extends Reducer> getReducerClass() {
		return TheReducer.class;
	}

	public static class TheMapper
			extends
			Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
		private final GenericRecord key;
		private final GenericRecord value;

		public TheMapper() {
			key = new GenericData.Record(KEY_SCHEMA);
			value = new GenericData.Record(VALUE_SCHEMA);
			value.put("count", 1L);
		}

		@Override
		protected void map(AvroKey<GenericRecord> input, NullWritable unused,
				Context context) throws IOException, InterruptedException {
			key.put("identifier", input.datum().get("id"));
			System.out.println("Input key=" + key.get("identifier"));
			context.write(new AvroKey<GenericRecord>(key),
					new AvroValue<GenericRecord>(value));
		}
	}

	public static class TheReducer
			extends
			Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
		private final GenericRecord output;

		public TheReducer() {
			output = new GenericData.Record(OUTPUT_SCHEMA);
		}

		@Override
		protected void reduce(AvroKey<GenericRecord> key,
				Iterable<AvroValue<GenericRecord>> values, Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			for (AvroValue<GenericRecord> value : values) {
				count += (Long) value.datum().get("count");
			}
			output.put("identifier", key.datum().get("identifier"));
			output.put("count", count);
			System.out.println("Output key=" + output.get("identifier") + "  count="
					+ output.get("count"));
			context.write(new AvroKey<GenericRecord>(output), null);
		}
	}
}
