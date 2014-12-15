package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RCFileReducer extends Reducer<Text, IntWritable, Text, Text> {

    Text one = new Text("1");
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        for(IntWritable val : values){
            context.write(key, one);
        }
    }

}
