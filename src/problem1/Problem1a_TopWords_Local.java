package problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Problem1a_TopWords_Local {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<String> stopwords = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())));
                    String line;

                    while ((line = br.readLine()) != null) {
                        stopwords.add(line.trim());
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase().replaceAll("[^a-z ]", " ");
            String[] tokens = line.split("\\s+");

            for (String token : tokens) {
                if (token.length() > 2 && !stopwords.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: <input> <output> <stopwords>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // 🔥 LOCAL MODE
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "Top Words Local");

        job.setJarByClass(Problem1a_TopWords_Local.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(ReducerClass.class);  // optimization

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI("file://" + args[2]));

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("file://" + args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path("file://" + args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}