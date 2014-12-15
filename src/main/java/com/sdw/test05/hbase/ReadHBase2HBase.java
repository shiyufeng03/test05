package com.sdw.test05.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadHBase2HBase extends Configured implements Tool {

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
                InterruptedException {
            Put put = new Put(row.get());
            for(KeyValue kv : value.list()){
                put.add(kv);
            }
            context.write(row, put);
        }
    }
    
    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseUtils.getConf(), new ReadHBase2HBase(), args);
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.out.println("Usage: <sourceHBaseTableName> <targetHBaseTableName>");
            return -1;
        }
        
        Configuration conf = getConf();
        Job job = new Job(conf, "test-hbase2hbase");
        
        job.setJarByClass(ReadHBase2HBase.class);
        
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.setCacheBlocks(false);
        
        TableMapReduceUtil.initTableMapperJob(args[0], scan, MyMapper.class, null, null, job);
        
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);
        
        job.setNumReduceTasks(0);
        
        boolean result = job.waitForCompletion(true);
        if(result == false){
            throw new RuntimeException("job fail");
        }else{
            return 0;
        }
    }

}
