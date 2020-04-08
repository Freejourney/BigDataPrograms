package bigdata.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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