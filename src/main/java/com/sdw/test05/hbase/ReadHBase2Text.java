package com.sdw.test05.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadHBase2Text extends Configured implements Tool {

    public static class MyMapper extends TableMapper<Text, LongWritable> {
        private LongWritable one = new LongWritable(1);

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
                InterruptedException {
            for (KeyValue kv : value.list()) {
                context.write(new Text(Bytes.toString(kv.getRow())), one);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseUtils.getConf(), new ReadHBase2Text(), args);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: <tableName> <outdir>");
        }

        Configuration conf = getConf();

        Job job = new Job(conf, "test-readhbase-textfile");
        job.setJarByClass(ReadHBase2Text.class);

        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(args[0], scan, MyMapper.class, Text.class, LongWritable.class, job);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean result = job.waitForCompletion(true);
        
        if(result == false){
            throw new RuntimeException("job fail.");
        }else{
            return 0;
        }
    }

}
