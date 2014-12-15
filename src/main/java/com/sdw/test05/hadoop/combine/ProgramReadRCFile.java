package com.sdw.test05.hadoop.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProgramReadRCFile {

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.out.println("--<intput> <output>");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Job job = new Job(conf, "read-rcfile");

        job.setJarByClass(ProgramReadRCFile.class);
        job.setInputFormatClass(LogRCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        LogRCFileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapperClass(ReadTestMapper.class); 
        
        boolean result = job.waitForCompletion(true);
    }

    public static class ReadTestMapper extends Mapper<LongWritable, BytesRefArrayWritable, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            Text txt = new Text();
            // 因为RcFile行存储和列存储，所以每次进来的一行数据，Value是个列簇，遍历，输出。
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < value.size(); i++) {
                BytesRefWritable v = value.get(i);
                txt.set(v.getData(), v.getStart(), v.getLength());
                if (i == value.size() - 1) {
                    sb.append(txt.toString());
                } else {
                    sb.append(txt.toString() + "\t");
                }
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

}
