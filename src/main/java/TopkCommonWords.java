import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class TopkCommonWords {

    public static class CountWords_1
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            ArrayList<String> stopWords = TopkCommonWords.setup(context);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                if (stopWords.contains(word.toString())) {
                    continue;
                }
                context.write(word, one);
            }
        }
    }

    public static class CountWords_2
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable two = new IntWritable(2);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            ArrayList<String> stopWords = TopkCommonWords.setup(context);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWords.contains(word.toString())) {
                    continue;
                }
                context.write(word, two);
            }
        }
    }

    public static class SortFrequencies
            extends Mapper<Object, Text, Text, LongWritable>{

        private TreeSet<WordAndCountTuple> tset;

        public void setup(Context context) throws IOException, InterruptedException {
            tset = new TreeSet<>(WordAndCountTuple::compareTo);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");

            String word = tokens[0];
            long frequency = Long.parseLong(tokens[1]);

            WordAndCountTuple wact = new WordAndCountTuple(frequency, word);
            tset.add(wact);

            if (tset.size() > 20) {
                tset.remove(tset.first());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (WordAndCountTuple entry : tset.descendingSet()) {
                long count = entry.getKey();
                String name = entry.getValue();

                context.write(new Text(name), new LongWritable(count));
            }
        }
    }

    public static class WordAndCountTuple implements Comparable<WordAndCountTuple> {
        public Long freq;
        public String word;

        public WordAndCountTuple(Long freq, String word) {
            this.freq = freq;
            this.word = word;
        }

        public Long getKey() {
            return this.freq;
        }

        public String getValue() {
            return this.word;
        }

        @Override
        public int compareTo(WordAndCountTuple wact) {
            if (wact == null) {
                return 1;
            } else {
                int result = (this.freq).compareTo(wact.freq);
                if (result == 0) {
                    return (this.word).compareTo(wact.word);
                } else {
                    return result;
                }
            }
        }
    }

    public static class SortingReducer
            extends Reducer<Text,LongWritable,LongWritable,Text> {
        private TreeSet<WordAndCountTuple> tset;

        public void setup(Context context) throws IOException, InterruptedException {
            tset = new TreeSet<>(WordAndCountTuple::compareTo);
        }

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String word = key.toString();
            long count = 0;

            for (LongWritable val: values) {
                count = val.get();
            }

            WordAndCountTuple wact = new WordAndCountTuple(count, word);
            tset.add(wact);

            if (tset.size() > 20) {
                tset.remove(tset.first());
            }
        }



        public void cleanup(Context context) throws IOException, InterruptedException {
            for (WordAndCountTuple entry : tset.descendingSet()) {
                long count = entry.getKey();
                String name = entry.getValue();
                context.write(new LongWritable(count), new Text(name));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum1 = 0;
            int sum2 = 0;

            for (IntWritable val : values) {
                if (val.get() == 1) {
                    sum1 += 1;
                } else {
                    sum2 += 1;
                }
            }

            int resultValue = Math.min(sum1, sum2);
            result.set(resultValue);

            if (resultValue != 0) {
                context.write(key, result);
            }
        }
    }

    public static ArrayList<String> setup(Mapper.Context context) throws IOException {
        ArrayList<String> stopWords = new ArrayList<String>();

        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                String line = "";

                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                while ((line = reader.readLine()) != null) {
                    String[] words = line.split("\n");

                    for (int i = 0; i < words.length; i++) {
                        stopWords.add(words[i]);
                    }
                }
            } catch (Exception e) {
                System.out.println("Unable to read the File");
                System.exit(1);
            }
        }
        return stopWords;
    }


    public static void main(String[] args) throws Exception {
        // Job 1
        Configuration conf = new Configuration();
        Job job_1 = Job.getInstance(conf, "find the frequency of common words between the files");
        job_1.setJarByClass(TopkCommonWords.class);
        job_1.setMapperClass(TopkCommonWords.CountWords_1.class);
        job_1.setReducerClass(TopkCommonWords.IntSumReducer.class);
        job_1.setMapOutputKeyClass(Text.class);
        job_1.setMapOutputValueClass(IntWritable.class);
        /**
         * Add cache file to all nodes
         */
        job_1.addCacheFile(new Path(args[2]).toUri());
        MultipleInputs.addInputPath(job_1, new Path(args[0]),
                TextInputFormat.class, CountWords_1.class);
        MultipleInputs.addInputPath(job_1, new Path(args[1]),
                TextInputFormat.class, CountWords_2.class);
        FileOutputFormat.setOutputPath(job_1, new Path("commonwords/tmp"));
        job_1.waitForCompletion(true);

        // job2
        Job job_2 = Job.getInstance(conf, "use a treemap to sort the top 10 in descending order");
        job_2.setJarByClass(TopkCommonWords.class);
        job_2.setInputFormatClass(TextInputFormat.class);
        job_2.setOutputFormatClass(TextOutputFormat.class);

        job_2.setMapperClass(TopkCommonWords.SortFrequencies.class);
        job_2.setReducerClass(TopkCommonWords.SortingReducer.class);

        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job_2, new Path("commonwords/tmp"));
        FileOutputFormat.setOutputPath(job_2, new Path(args[3]));
        System.exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
}
