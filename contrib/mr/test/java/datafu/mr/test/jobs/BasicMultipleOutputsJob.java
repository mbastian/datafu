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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;
import datafu.mr.jobs.StagedOutputJob;

public class BasicMultipleOutputsJob extends AbstractJob {

	@Override
	public void setupInputFormat(StagedOutputJob job) throws IOException {
		job.setInputFormatClass(SequenceFileInputFormat.class);
	}
	
	@Override
	public void setupIntermediateFormat(StagedOutputJob job) throws IOException {	
	}

	@Override
	public void setupOutputFormat(StagedOutputJob job) throws IOException {
		MultipleOutputs.addNamedOutput(job, "foo",
				SequenceFileOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "bar",
				SequenceFileOutputFormat.class, IntWritable.class, Text.class);
	}

	@Override
	public Class<? extends Mapper> getMapperClass() {
		return Mapper.class;
	}

	@Override
	public Class<? extends Reducer> getReducerClass() {
		return MOReduce.class;
	}

	@Override
	protected Class<?> getMapOutputKeyClass() {
		return IntWritable.class;
	}

	@Override
	protected Class<?> getMapOutputValueClass() {
		return Text.class;
	}

	public static class MOReduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private MultipleOutputs<IntWritable, Text> mos;

		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs<IntWritable, Text>(context);
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				mos.write(val.toString(), key, val, val.toString() + "/part");
			}
		}

		public void cleanup(Context c) throws IOException, InterruptedException {
			mos.close();
		}
	}
}