package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LogRCFileRecordReader<K extends LongWritable, V extends BytesRefArrayWritable> extends
        RecordReader<LongWritable, BytesRefArrayWritable> {

    private RCFileRecordReader<LongWritable, BytesRefArrayWritable> _recordReader;

    private LongWritable key;

    private BytesRefArrayWritable value;

    public LogRCFileRecordReader() {

    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
            (org.apache.hadoop.mapreduce.lib.input.FileSplit) split;

        FileSplit split2 =
            new FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());

        this._recordReader = new RCFileRecordReader<LongWritable, BytesRefArrayWritable>(conf, split2);
        this.key = this._recordReader.createKey();
        this.value = this._recordReader.createValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this._recordReader.next(key, value);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesRefArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this._recordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this._recordReader.close();
    }

}
