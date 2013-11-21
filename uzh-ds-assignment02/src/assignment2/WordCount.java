/*
 * Distributed Systems 2013
 * Assignment 2: Word-Count
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount
        extends Configured
        implements Tool {

    private static final String INPUT_LOCATION = "/user/ds2013/data/plot_summaries.txt";


    public static void main(String[] pArgs)
            throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), pArgs);
        System.exit(res);
    }


    @Override
    public int run(String[] pArgs)
            throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("wordcount");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(CombinerClass.class);
        conf.setReducerClass(ReducerClass.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(INPUT_LOCATION));
        FileSystem.get(conf).delete(new Path(pArgs[0]), true);
        FileOutputFormat.setOutputPath(conf, new Path(pArgs[0]));

        JobClient.runJob(conf);

        return 0;
    }


    public static class CombinerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text pKey, Iterator<IntWritable> pValues, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            int sum = 0;
            while (pValues.hasNext()) {
                sum += pValues.next().get();
            }
            pOutput.collect(new Text(""), new IntWritable(sum));
        }

    }

    public static class ReducerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, NullWritable, IntWritable> {

        @Override
        public void reduce(Text pKey, Iterator<IntWritable> pValues, OutputCollector<NullWritable, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            int sum = 0;
            while (pValues.hasNext()) {
                sum += pValues.next().get();
            }
            pOutput.collect(NullWritable.get(), new IntWritable(sum));
        }

    }

}
