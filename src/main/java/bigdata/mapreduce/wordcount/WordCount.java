package bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ua28 on 4/6/20.
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1
        Configuration conf = new Configuration();
//        conf.set("mapred.map.tasks", "4000");

        // 2
        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");

        // 3
        job.setJarByClass(WordCount.class);

        // 4
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // 5
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 当数据集很小且文件只有一个时只会有一个Mapper(), 现在会有三个Mapper，因为有三个小文件
        FileInputFormat.addInputPath(job, new Path("resource/milano_temps.csv"));
        FileOutputFormat.setOutputPath(job, new Path("mapreducewordcount_result"));

        // 7
        job.waitForCompletion(true);
    }
}
