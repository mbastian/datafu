package datafu.mr.jobs;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.mr.avro.AvroMultipleInputsKeyInputFormat;
import datafu.mr.avro.AvroMultipleInputsUtil;
import datafu.mr.avro.CombinedAvroKeyInputFormat;
import datafu.mr.avro.CombinedAvroMultipleInputsKeyInputFormat;
import datafu.mr.fs.PathUtils;

public abstract class AbstractAvroJob extends AbstractJob {

	private final Logger _log = Logger.getLogger(AbstractAvroJob.class);

	private boolean _combineInputs;

	public AbstractAvroJob() {
		setConf(new Configuration());
	}

	public AbstractAvroJob(String name, Properties props) throws IOException {
		super(name, props);

		if (props.containsKey("combine.inputs")) {
			setCombineInputs(Boolean.parseBoolean(props
					.getProperty("combine.inputs")));
		}
	}

	protected abstract Schema getReduceOutputSchema();

	protected Schema getMapOutputKeySchema() {
		return null;
	}

	protected Schema getMapOutputValueSchema() {
		return null;
	}

	@Override
	public void setupInputFormat(StagedOutputJob job) throws IOException {
		if (job.getInputPaths().size() > 1) {
			if (_combineInputs) {
				job.setInputFormatClass(CombinedAvroMultipleInputsKeyInputFormat.class);
			} else {
				job.setInputFormatClass(AvroMultipleInputsKeyInputFormat.class);
			}

			for (Path p : getInputPaths()) {
				Schema inputSchema = PathUtils.getSchemaFromPath(
						getFileSystem(), p);
				AvroMultipleInputsUtil.setInputKeySchemaForPath(job,
						inputSchema, p.toString());
			}
		} else {
			if (_combineInputs) {
				job.setInputFormatClass(CombinedAvroKeyInputFormat.class);
			} else {
				job.setInputFormatClass(AvroKeyInputFormat.class);
			}

			Schema inputSchema = PathUtils.getSchemaFromPath(getFileSystem(),
					getInputPaths().get(0));
			AvroJob.setInputKeySchema(job, inputSchema);
		}
	}

	@Override
	public void setupIntermediateFormat(StagedOutputJob job) throws IOException {
		if (AvroKey.class.isAssignableFrom(IntermediateTypeHelper
				.getMapperOutputKeyClass(getMapperClass()))) {
			if (getMapOutputKeySchema() != null) {
				AvroJob.setMapOutputKeySchema(job, getMapOutputKeySchema());
			} else {
				// Infer schema
			}
		}
		if (AvroValue.class.isAssignableFrom(IntermediateTypeHelper
				.getMapperOutputValueClass(getMapperClass()))) {
			if (getMapOutputValueSchema() != null) {
				AvroJob.setMapOutputValueSchema(job, getMapOutputValueSchema());
			} else {
				// Infer schema
			}
		}
	}

	@Override
	public void setupOutputFormat(StagedOutputJob job) throws IOException {
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setOutputKeySchema(job, getReduceOutputSchema());
	}

	/**
	 * Gets whether inputs should be combined.
	 * 
	 * @return true if inputs are to be combined
	 */
	public boolean getCombineInputs() {
		return _combineInputs;
	}

	/**
	 * Sets whether inputs should be combined.
	 * 
	 * @param combineInputs
	 *            true to combine inputs
	 */
	public void setCombineInputs(boolean combineInputs) {
		_combineInputs = combineInputs;
	}
}
