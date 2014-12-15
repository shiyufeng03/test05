package com.sdw.test05.hadoop.combine;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/*
 * written by frankgu
 * referenced hadoop old API
 * MultipleOutputFormat
 */
public abstract class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends FileOutputFormat<K, V> {
	
	private static Logger logger = Logger.getLogger(MultipleOutputFormat.class);
	private static final NumberFormat TASKID_FORMAT = NumberFormat.getInstance();
	static {
		TASKID_FORMAT.setMinimumIntegerDigits(5);
		TASKID_FORMAT.setGroupingUsed(false);
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		/*
		 * setup main path
		 */
		Path path = null;
		OutputCommitter committer = super.getOutputCommitter(context);
		if (committer instanceof FileOutputCommitter) {
			path = ((FileOutputCommitter) committer).getWorkPath();
		} else {
			path = super.getOutputPath(context);
			if (path == null)
				throw new IOException("Undefined job output-path");
		}

		final Path workPath = path;
		final TaskAttemptContext taskAttemptContext = context;

		/*
		 * this adaptive writer, record paths by tree_map
		 */
		return new RecordWriter<K, V>() {

			// a cache storing the record writers for different output files.
			TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();

			// adapt writing
			public synchronized void write(K key, V value) throws IOException,
					InterruptedException {

				// get full file path
				String filename = String.format(
						"%s-r-%s-%s",
						generateFileNameForKeyValue(key, value),
						TASKID_FORMAT.format(taskAttemptContext
								.getTaskAttemptID().getTaskID().getId()),
						taskAttemptContext.getTaskAttemptID().getId());
				Path fullPath = new Path(workPath, filename);

				RecordWriter<K, V> writer = recordWriters.get(filename);
				if (writer == null) {
					// if we don't have the record writer yet for the final
					// path, create one,
					// and add it to the cache
					logger.info(String.format("create writer for path : %s", fullPath.toString()));
					writer = getRealRecordWriter(taskAttemptContext, fullPath);
					this.recordWriters.put(filename, writer);
				}
				writer.write(key, value);
			}

			// close real writers
			public synchronized void close(TaskAttemptContext taskAttemptContext) throws IOException,
					InterruptedException {
				for (Map.Entry<String, RecordWriter<K, V>> entry : this.recordWriters.entrySet()) {
					logger.info(String.format("close writer for path : %s", entry.getKey()));
					RecordWriter<K, V> writer = entry.getValue();
					writer.close(taskAttemptContext);
				}
				this.recordWriters.clear();
			}
		};
	}

	protected abstract RecordWriter<K, V> getRealRecordWriter(
			TaskAttemptContext context, Path fullPath) throws IOException,
			InterruptedException;

	protected abstract String generateFileNameForKeyValue(K key, V value);
}
