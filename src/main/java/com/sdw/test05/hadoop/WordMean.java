package com.sdw.test05.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

public class WordMean extends Configured implements Tool {

    private final static Text COUNT = new Text("count");

    private final static Text LENGTH = new Text("length");

    private final static LongWritable ONE = new LongWritable(1);

    private double mean = 0;

    public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable wordlen = new LongWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            while (token.hasMoreTokens()) {
                String str = token.nextToken();
                this.wordlen.set(str.length());

                context.write(LENGTH, this.wordlen);
                context.write(COUNT, ONE);
            }
        }
    }

    public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sum = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
                InterruptedException {
            long theSum = 0;
            for (LongWritable val : values) {
                theSum += val.get();
            }

            sum.set(theSum);
            context.write(key, sum);
        }
    }

    private double readAndCalcMean(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        Path resultPath = null;
        for (FileStatus status : fs.listStatus(path)) {
            if (status.isFile() && status.getPath().getName().matches("part-.*")) {
                resultPath = status.getPath();
                break;
            }
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(fs.open(resultPath), Charsets.UTF_8));

            long count = 0;
            long length = 0;

            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line);

                String type = st.nextToken();

                if (type.equals(COUNT.toString())) {
                    String countStr = st.nextToken();

                    count = Long.parseLong(countStr);
                } else if (type.equals(LENGTH.toString())) {
                    String lengthStr = st.nextToken();

                    length = Long.parseLong(lengthStr);
                }
            }

            double theMean = ((double) length) / ((double) count);
            System.out.println("The mean is: " + theMean);

            return theMean;
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordMean(), args);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: wordmean <in> <out>");
            return 0;
        }

        for (String arg : args) {
            System.out.println("arg: " + arg);
        }

        Configuration conf = getConf();

        Job job = new Job(conf, "wordmean");
        job.setJarByClass(WordMean.class);

        job.setMapperClass(WordMeanMapper.class);
        job.setReducerClass(WordMeanReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        mean = this.readAndCalcMean(outputPath, conf);

        return (result ? 0 : 1);
    }

    public double getMean() {
        return this.mean;
    }

}
