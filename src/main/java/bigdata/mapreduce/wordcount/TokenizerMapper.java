package bigdata.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 */

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private int tmp = 0;

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

        System.out.println(tmp++);

        String cleanline = value.toString().toLowerCase().replaceAll(" \"", " ");
        StringTokenizer itr = new StringTokenizer(cleanline);
        while (itr.hasMoreTokens())
            context.write(new Text(itr.nextToken().trim()), new IntWritable(1));
    }
}