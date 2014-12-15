package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LogRCFileOutputFormat extends MultipleOutputFormat<Text, Text>{

    @Override
    protected RecordWriter<Text, Text> getRealRecordWriter(TaskAttemptContext context, Path fullPath)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        return new ValRCFileWriter<Text, Text> (conf,
                GzipCodec.class,
                fullPath);
    }

    @Override
    protected String generateFileNameForKeyValue(Text key, Text value) {
        return "file";
    }

}
