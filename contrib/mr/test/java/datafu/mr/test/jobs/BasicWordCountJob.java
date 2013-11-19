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
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;
import datafu.mr.jobs.StagedOutputJob;

public class BasicWordCountJob extends AbstractJob {

	@Override
	public void setupInputFormat(StagedOutputJob job) throws IOException {
		job.setInputFormatClass(SequenceFileInputFormat.class);
	}
	
	@Override
	public void setupIntermediateFormat(StagedOutputJob job) throws IOException {	
	}

	@Override
	public void setupOutputFormat(StagedOutputJob job) throws IOException {
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	}

	@Override
	public Class<? extends Mapper> getMapperClass() {
		return Map.class;
	}

	@Override
	public Class<? extends Reducer> getReducerClass() {
		return Reduce.class;
	}
	
	@Override
	protected Class<?> getMapOutputKeyClass() {
		return Text.class;
	}
	
	@Override
	protected Class<?> getMapOutputValueClass() {
		return IntWritable.class;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("MAPPER INPUT : "+key.toString()+" : "+value.toString());
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println("REDUCER INPUT : "+key.toString());
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
