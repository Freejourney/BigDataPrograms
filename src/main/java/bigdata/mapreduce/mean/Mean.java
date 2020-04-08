package bigdata.mapreduce.mean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ua28 on 4/8/20.
 */
public class Mean {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
//
//        Job job = Job.getInstance(conf);
//        job.setJobName("Mean");
//
//        job.setJarByClass(Mean.class);
//
//        job.setMapperClass();
//        job.setReducerClass();
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.addInputPath(job, new Path(""));
//        FileOutputFormat.setOutputPath(job, new Path(""));
//
//        job.waitForCompletion(true);
    }

    public static class SumMapper extends Mapper<Text, Text, Text, IntWritable> {

        private int date = 0;
        private int min = 1;
        private int max = 2;

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            String sdate = data[date];
            double dmin = Double.valueOf(data[min]);
            double dmax = Double.valueOf(data[max]);


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
