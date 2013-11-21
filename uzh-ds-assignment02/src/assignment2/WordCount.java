/*
 * Distributed Systems 2013
 * Assignment 2: Word-Count
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
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

    private static final String STOPWORDS_LOCATION = "/user/ds2013/stop_words/english_stop_list.txt";
    private static final String INPUT_LOCATION     = "/user/ds2013/data/plot_summaries.txt";


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

        conf.setMapperClass(MapClass.class);
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

    public static class MapClass
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one        = new IntWritable(1);
        private final Set<String>        mStopWords = new HashSet<String>();

        private Text                     word       = new Text();

        @Override
        public void map(LongWritable pKey, Text pValue, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            String line = Utils.removeSpecialChars(Utils.nullsafeLowercase(pValue.toString()));
            StringTokenizer itr = new StringTokenizer(line);
            if (itr.hasMoreTokens()) {
                itr.nextToken(); // ignore as this is usually the movie number
            }
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (!mStopWords.contains(token)) {
                    word.set(token);
                    pOutput.collect(word, one);
                }
            }
        }

        @Override
        public void configure(JobConf pJob) {
            // Parse stop-words
            try {
                FileSystem fs = FileSystem.get(pJob);
                Path p = new Path(STOPWORDS_LOCATION);

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
                String word = null;
                while ((word = reader.readLine()) != null) {
                    mStopWords.add(word);
                }
                reader.close();
            } catch (Exception pEx) {
                System.err.println("Error in parsing skip lines, please change to the connecting train");
                pEx.printStackTrace();
            }
        }

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
