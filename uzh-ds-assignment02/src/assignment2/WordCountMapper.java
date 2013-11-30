/*
 * Distributed Systems 2013
 * Internal class: WordCountMapper
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WordCountMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE        = new IntWritable(1);

    private Set<String>              mStopWords = new HashSet<String>();
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
                pOutput.collect(word, ONE);
            }
        }
    }

    @Override
    public void configure(JobConf pJob) {
        mStopWords = Utils.parseStopWords(pJob);
    }

}
