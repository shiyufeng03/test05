package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;


public class ValRCFileWriter<K, V> extends RecordWriter<K, V> {
    public final static String FIELD_DELIMITED = "\\t";
    protected RCFile.Writer out;
	
	/*
	 * buffered output array for output
	 */
	private final BytesRefArrayWritable array;
	private final int numColumns;
	
	/**
	 * construct
	 * @param conf
	 * @param codecClass
	 * @param path
	 * @throws IOException
	 */
	public ValRCFileWriter(Configuration conf ,
			Class<?> codecClass, Path path) throws IOException {

		FileSystem fs = path.getFileSystem(conf);
		CompressionCodec codec = (CompressionCodec) 
			ReflectionUtils.newInstance(codecClass, conf);
		
		this.out = new RCFile.Writer(fs, conf, path, null, codec);
		numColumns = conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
		this.array = new BytesRefArrayWritable(numColumns);
	}
	
	@Override
	public synchronized void write(K key, V value) throws IOException {
		
		String[] fileds = key.toString().split(FIELD_DELIMITED, -1);
		for (int i = 0; i < fileds.length && i < this.numColumns; i++) {
			array.set(i, new BytesRefWritable(fileds[i].getBytes("UTF-8")));
		}
		out.append(array);
	}

	@Override
	public synchronized void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		out.close();		
	}
}
