/*
 * Distributed Systems 2013
 * Assignment 2: Join
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Join
        extends Configured
        implements Tool {

    private static final String INPUT_CHARACTER_LOCATION = "/user/ds2013/data/character.metadata.tsv";
    private static final String INPUT_MOVIE_LOCATION     = "/user/ds2013/data/movie.metadata.tsv";


    public static void main(String[] pArgs)
            throws Exception {
        int res = ToolRunner.run(new Configuration(), new Join(), pArgs);
        System.exit(res);
    }


    @Override
    public int run(String[] pArgs)
            throws Exception {
        // Validate CLI args
        int numOfReducers = -1;
        if (pArgs.length < 1) {
            throw new IllegalArgumentException("You need to set the output path.");
        } else if (pArgs.length > 1) {
            try {
                numOfReducers = Integer.parseInt(pArgs[1]);
                if (numOfReducers < 0) {
                    throw new Exception(); // is catched in this block
                }
            } catch (Exception pEx) {
                throw new IllegalArgumentException("Second argument (number of reducers) must be a positive (or 0) integer.");
            }
        }

        JobConf conf = new JobConf(getConf(), Join.class);
        conf.setJobName("join");

        if (numOfReducers >= 0) {
            conf.setNumReduceTasks(numOfReducers);
        }

        // Setting the key value types for mapper/reducer outputs
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Specifying the single reducer class
        conf.setReducerClass(MovieReducerClass.class);

        // Specifying input directories and Mappers from multiple sources
        MultipleInputs.addInputPath(conf, new Path(INPUT_CHARACTER_LOCATION), TextInputFormat.class, ActorMapper.class);
        MultipleInputs.addInputPath(conf, new Path(INPUT_MOVIE_LOCATION), TextInputFormat.class, MovieMapper.class);

        // Specifying output directory and remove the old file before writing to it
        FileSystem.get(conf).delete(new Path(pArgs[0]), true);
        conf.setOutputFormat(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(conf, new Path(pArgs[0]));

        JobClient.runJob(conf);

        return 0;
    }


    public static class ActorMapper
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private Text mMovieId   = new Text();
        private Text mActorName = new Text();

        @Override
        public void map(LongWritable pKey, Text pValue, OutputCollector<Text, Text> pOutput, Reporter pReporter)
                throws IOException {
            String[] split = pValue.toString().split("\\t");
            // 0. Wikipedia movie ID
            // 8. Actor name
            mMovieId.set(split[0]);
            String an = split[8];
            if ((an != null) && !an.trim().isEmpty()) {
                mActorName.set(new StringBuilder("AN::").append(an.trim()).toString());
                pOutput.collect(mMovieId, mActorName);
            }
        }
    }

    public static class MovieMapper
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private Text mMovieId   = new Text();
        private Text mMovieName = new Text();

        @Override
        public void map(LongWritable pKey, Text pValue, OutputCollector<Text, Text> pOutput, Reporter pReporter)
                throws IOException {
            String[] split = pValue.toString().split("\\t");
            // 0. Wikipedia movie ID
            // 2. Movie name
            mMovieId.set(split[0]);
            String mn = split[2];
            if ((mn != null) && !mn.trim().isEmpty()) {
                mMovieName.set(new StringBuilder("MN::").append(mn.trim()).toString());
                pOutput.collect(mMovieId, mMovieName);
            }
        }
    }

    public static class MovieReducerClass
            extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        private static final String SEPARATOR = "\t";

        @Override
        public void reduce(Text pKey, Iterator<Text> pValues, OutputCollector<Text, Text> pOutput, Reporter pReporter)
                throws IOException {

            String movieName = "";
            Set<String> actor = new HashSet<String>();
            while (pValues.hasNext()) {
                String value = pValues.next().toString();
                String[] split = value.split("::");
                if ("AN".equals(split[0])) {
                    // is character name
                    actor.add(split[1].trim());
                } else if ("MN".equals(split[0])) {
                    // is movie name
                    movieName = split[1].trim();
                } else {
                    System.err.println("ERR: \"" + value + "\" could not be mapped.");
                }
            }

            if (movieName.isEmpty()) {
                System.err.println("WARN: Movie Name is empty for movie id " + pKey.toString());
            }

            StringBuilder sb = new StringBuilder(movieName);
            for (String a : actor) {
                sb.append(SEPARATOR);
                sb.append(a);
            }
            pOutput.collect(pKey, new Text(sb.toString()));
        }

    }

}
