/*
 * Distributed Systems 2013
 * Assignment 2: Freq-Words
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FreqWords
        extends Configured
        implements Tool {

    private static final String INPUT_LOCATION = "/user/ds2013/data/plot_summaries.txt";
    private static final String CACHE_LOCATION = "/user/ds2013/tmp/freqwords-tmp";


    public static void main(String[] pArgs)
            throws Exception {
        int res = ToolRunner.run(new Configuration(), new FreqWords(), pArgs);
        System.exit(res);
    }


    @Override
    public int run(String[] pArgs)
            throws Exception {
        JobConf jobCount = new JobConf(getConf(), FreqWords.class);
        jobCount.setJobName("freqwords-extract");

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.setMapperClass(WordCountMapper.class);
        jobCount.setCombinerClass(ReducerClass.class);
        jobCount.setReducerClass(ReducerClass.class);

        jobCount.setInputFormat(TextInputFormat.class);
        jobCount.setOutputFormat(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(jobCount, new Path(INPUT_LOCATION));
        FileSystem.get(jobCount).delete(new Path(CACHE_LOCATION), true);
        SequenceFileOutputFormat.setOutputPath(jobCount, new Path(CACHE_LOCATION));

        JobConf jobSort = new JobConf(getConf(), FreqWords.class);
        jobSort.setJobName("freqwords-sort");

        jobSort.setMapOutputKeyClass(IntWritable.class);
        jobSort.setMapOutputValueClass(Text.class);

        jobSort.setOutputKeyClass(Text.class);
        jobSort.setOutputValueClass(IntWritable.class);

        jobSort.setMapperClass(SortMapperClass.class);
        jobSort.setReducerClass(SortReducerClass.class);

        jobSort.setInputFormat(SequenceFileInputFormat.class);
        jobSort.setOutputFormat(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(jobSort, new Path(CACHE_LOCATION));
        FileSystem.get(jobSort).delete(new Path(pArgs[0]), true);
        FileOutputFormat.setOutputPath(jobSort, new Path(pArgs[0]));

        jobSort.setOutputKeyComparatorClass(ReverseIntWritableComparator.class);


        // Actually run jobs
        JobClient.runJob(jobCount);
        JobClient.runJob(jobSort);

        // Remove temporary file after finish
        FileSystem.get(jobCount).delete(new Path(CACHE_LOCATION), true);

        return 0;
    }


    public static class ReducerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text pKey, Iterator<IntWritable> pValues, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            int sum = 0;
            while (pValues.hasNext()) {
                sum += pValues.next().get();
            }
            pOutput.collect(pKey, new IntWritable(sum));
        }

    }

    public static class SortMapperClass
            extends MapReduceBase
            implements Mapper<Text, IntWritable, IntWritable, Text> {

        @Override
        public void map(Text pKey, IntWritable pValue, OutputCollector<IntWritable, Text> pOutput, Reporter pReporter)
                throws IOException {
            pOutput.collect(pValue, pKey);
        }

    }

    public static class SortReducerClass
            extends MapReduceBase
            implements Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(IntWritable pKey, Iterator<Text> pValues, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            while (pValues.hasNext()) {
                pOutput.collect(pValues.next(), pKey);
            }
        }

    }


    public static class ReverseIntWritableComparator
            extends IntWritable.Comparator {

        public ReverseIntWritableComparator() {
            super();
        }

        @Override
        public int compare(byte[] pB1, int pS1, int pL1, byte[] pB2, int pS2, int pL2) {
            return -1 * super.compare(pB1, pS1, pL1, pB2, pS2, pL2);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable pA, WritableComparable pB) {
            return -1 * super.compare(pA, pB);
        }

    }

}
