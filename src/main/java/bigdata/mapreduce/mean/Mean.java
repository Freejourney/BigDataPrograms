package bigdata.mapreduce.mean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ua28 on 4/8/20.
 */
public class Mean {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("Mean");

        job.setJarByClass(Mean.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(MeanReduer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("resource/milano_temps.csv"));
        FileOutputFormat.setOutputPath(job, new Path("mapreducemean_result"));

        System.out.println(job.waitForCompletion(true));
    }

    /**
     * map()的输入是LongWritable类型的，不能用Text类型表示，可以用Object或者LongWritable
     */
    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private int date = 0;
        private int min = 1;
        private int max = 2;

        private Map<String, List<Double>> maxsMap = new HashMap<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            String sdate = data[date];
            double dmin = Double.valueOf(data[min]);
            double dmax = Double.valueOf(data[max]);

            if (!maxsMap.containsKey(sdate)) {
                maxsMap.put(sdate, new ArrayList<Double>());
            }

            maxsMap.get(sdate).add(dmax);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String tdate : maxsMap.keySet()) {

                List<Double> tMaxs = maxsMap.get(tdate);

                double sum = 0.0d;
                for (double tmax : tMaxs) {
                    sum += tmax;
                }

                context.write(new Text(tdate), new DoubleWritable(sum/tMaxs.size()));
            }
        }
    }

    public static class MeanReduer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0d;
            int size = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                size++;
            }

            context.write(key, new DoubleWritable(sum/size));
        }
    }
}
