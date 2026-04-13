package problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Problem1c_Stripes_IMC_Local {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, MapWritable> {

        private Map<String, Map<String,Integer>> globalMap = new HashMap<>();
        private Set<String> topWords = new HashSet<>();
        private int distance;

        private static final int FLUSH_LIMIT = 5000;

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            distance = conf.getInt("d", 1);

            for (URI uri : context.getCacheFiles()) {
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

            String[] tokens = value.toString().toLowerCase().replaceAll("[^a-z ]"," ").split("\\s+");

            List<String> words = new ArrayList<>();
            for (String t : tokens) {

                String token = t.toLowerCase().replaceAll("[^a-z]", "");

                if (token.length() <= 1) continue;      // remove noise

                if (topWords.contains(token)) {
                    words.add(token);
                }
            }

            for (int i = 0; i < words.size(); i++) {

                String w1 = words.get(i);
                Map<String,Integer> stripe = globalMap.getOrDefault(w1, new HashMap<>());

                for (int j = Math.max(0,i-distance); j <= Math.min(words.size()-1,i+distance); j++) {

                    if (i == j) continue;

                    String w2 = words.get(j);
                    stripe.put(w2, stripe.getOrDefault(w2, 0) + 1);
                }

                globalMap.put(w1, stripe);

                if (globalMap.size() > FLUSH_LIMIT) {
                    flush(context);
                    globalMap.clear();
                }
            }
        }

        private void flush(Context context) throws IOException, InterruptedException {

            for (String key : globalMap.keySet()) {

                MapWritable stripeWritable = new MapWritable();
                Map<String,Integer> stripe = globalMap.get(key);

                for (String n : stripe.keySet()) {
                    stripeWritable.put(new Text(n), new IntWritable(stripe.get(n)));
                }

                context.write(new Text(key), stripeWritable);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            flush(context);
        }
    }

    public static class ReducerClass extends Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            MapWritable result = new MapWritable();

            for (MapWritable stripe : values) {
                for (Writable k : stripe.keySet()) {

                    IntWritable val = (IntWritable) stripe.get(k);

                    if (result.containsKey(k)) {
                        IntWritable existing = (IntWritable) result.get(k);
                        existing.set(existing.get() + val.get());
                    } else {
                        result.put(k, new IntWritable(val.get()));
                    }
                }
            }

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("d", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Stripes IMC Local");

        job.setJarByClass(Problem1c_Stripes_IMC_Local.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.addCacheFile(new URI("file://" + args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}