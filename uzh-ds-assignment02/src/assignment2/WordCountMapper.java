package assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    private static final String      STOPWORDS_LOCATION = "/user/ds2013/stop_words/english_stop_list.txt";
    private static final IntWritable ONE                = new IntWritable(1);

    private final Set<String>        mStopWords         = new HashSet<String>();
    private Text                     word               = new Text();


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
