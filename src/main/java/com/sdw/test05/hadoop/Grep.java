package com.sdw.test05.hadoop;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class Grep extends Configured implements Tool{

    public int run(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println("Usage: <inDir> <outDir> <regex> [<group>]");
            ToolRunner.printGenericCommandUsage(System.out);
            return 2;
        }
        
        Path tempDir = new Path("grep-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        
        System.out.println("tmp path: " + tempDir.getName());
        
        Configuration conf = getConf();
        conf.set(RegexMapper.PATTERN, args[2]);
        if(args.length == 4){
            conf.set(RegexMapper.GROUP, args[3]);
        }
        
        Job grepJob = new Job(conf, "grep-job");
        try{
            FileInputFormat.addInputPath(grepJob, new Path(args[0]));
            grepJob.setMapperClass(RegexMapper.class);
            grepJob.setCombinerClass(LongSumReducer.class);
            grepJob.setReducerClass(LongSumReducer.class);
            
            FileOutputFormat.setOutputPath(grepJob, tempDir);
            grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            grepJob.setOutputKeyClass(Text.class);
            grepJob.setOutputValueClass(LongWritable.class);
            
            grepJob.waitForCompletion(true);
            
            Job sortJob = new Job(conf, "grep-sort");
            
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            
            sortJob.setMapperClass(InverseMapper.class);

            sortJob.setNumReduceTasks(1);                 // write a single file
            FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
            sortJob.setSortComparatorClass(          // sort by decreasing freq
              LongWritable.DecreasingComparator.class);

            sortJob.waitForCompletion(true);
        }finally{
            
        }
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Grep(), args);
        System.exit(res);
      }

}
