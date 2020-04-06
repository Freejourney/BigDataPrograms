package bigdata.mapreduce;

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

        // 6
        FileInputFormat.addInputPath(job, new Path("pg201.txt"));
        FileOutputFormat.setOutputPath(job, new Path("result"));

        // 7
        job.waitForCompletion(true);
    }

    /**
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * StringTokenizer属于 java.util 包，用于分隔字符串.
         * StringTokenizer(String str) ：构造一个用来解析 str 的 StringTokenizer 对象。
         * java 默认的分隔符是空格("")、制表符(\t)、换行符(\n)、回车符(\r)。
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanline = value.toString().toLowerCase().replaceAll(" \"", " ");
            StringTokenizer itr = new StringTokenizer(cleanline);
            while (itr.hasMoreTokens())
                context.write(new Text(itr.nextToken().trim()), new IntWritable(1));
        }
    }

    /**
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
//            while (values.iterator().hasNext())
            for (IntWritable num : values) {
                sum += num.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}
