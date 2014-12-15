package com.sdw.test05.hadoop.outputtext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sdw.test05.hadoop.combine.LogRCFileInputFormat;
import com.sdw.test05.hadoop.combine.LogRCFileOutputFormat;
import com.sdw.test05.hadoop.combine.Program;
import com.sdw.test05.hadoop.combine.RCFileMapper;
import com.sdw.test05.hadoop.combine.RCFileReducer;

public class ProgramText {

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
        conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, 101);

        Job job = new Job(conf, "rcfile-combine");
        job.setJarByClass(Program.class);

        job.setInputFormatClass(LogRCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //LogRCFileInputFormat.addInputPath(job, new Path(args[0]));
        setInputPath(new Path(args[0]), conf, job);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RCFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*job.setReducerClass(RCFileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);*/

        boolean result = job.waitForCompletion(true);

    }

    private static void setInputPath(Path path, Configuration conf, Job job) throws FileNotFoundException, IOException {
        FileSystem fs = FileSystem.get(conf);

        for (Path tmpPath : getPath(fs, path)) {
            LogRCFileInputFormat.addInputPath(job, tmpPath);
        }
    }

    private static List<Path> getPath(FileSystem fs, Path path) throws FileNotFoundException, IOException {
        List<Path> paths = new ArrayList<Path>();

        if (fs.isFile(path)) {
            paths.add(path);
        } else {
            for (FileStatus status : fs.listStatus(path)) {
                if (status.isFile()) {
                    paths.add(status.getPath());
                } else {
                    paths.addAll(getPath(fs, status.getPath()));
                }
            }
        }
        return paths;
    }

}
