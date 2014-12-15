package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RCFileMapper extends Mapper<LongWritable, BytesRefArrayWritable, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    
    @Override
    protected void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException,
            InterruptedException {
        StringBuffer sb = new StringBuffer();  
        Text txt = new Text(); 
        for(int i=0;i<value.size();i++){
            BytesRefWritable v = value.get(i);  
            txt.set(v.getData(), v.getStart(), v.getLength());  
            if(i==value.size()-1){  
                sb.append(txt.toString());  
            }else{  
                sb.append(txt.toString()+"\t");  
            }  
        }
        context.write(new Text(sb.toString()), one);  
    }
}
