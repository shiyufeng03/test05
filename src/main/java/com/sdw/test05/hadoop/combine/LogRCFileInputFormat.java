package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LogRCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable> extends
        FileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        return new LogRCFileRecordReader();
    }

}
