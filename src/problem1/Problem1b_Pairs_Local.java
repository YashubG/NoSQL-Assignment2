package problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Problem1b_Pairs_Local {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<String> topWords = new HashSet<>();
        private int distance;

        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            distance = conf.getInt("d", 1);

            URI[] cacheFiles = context.getCacheFiles();

            for (URI uri : cacheFiles) {
                BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())));
                String line;

                while ((line = br.readLine()) != null) {
                    topWords.add(line.split("\t")[0]);
                }
                br.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase().replaceAll("[^a-z ]", " ");
            String[] tokens = line.split("\\s+");

            List<String> words = new ArrayList<>();

            for (String t : tokens) {
                if (topWords.contains(t)) words.add(t);
            }

            for (int i = 0; i < words.size(); i++) {

                String w1 = words.get(i);

                for (int j = Math.max(0, i - distance);
                     j <= Math.min(words.size() - 1, i + distance); j++) {

                    if (i == j) continue;

                    String w2 = words.get(j);

                    pair.set(w1 + "," + w2);
                    context.write(pair, one);
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

        if (args.length != 4) {
            System.err.println("Usage: <input> <output> <top50> <distance>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // 🔥 LOCAL MODE FIXES
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        conf.setInt("d", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Pairs Local");

        job.setJarByClass(Problem1b_Pairs_Local.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(ReducerClass.class); // 🔥 optimization

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 🔥 LOCAL CACHE FIX
        job.addCacheFile(new URI("file://" + args[2]));

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}