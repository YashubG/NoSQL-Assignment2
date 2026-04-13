package problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Problem1c_Stripes_Local {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, MapWritable> {

        private Set<String> topWords = new HashSet<>();
        private int distance;

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

                String token = t.toLowerCase().replaceAll("[^a-z]", "");

                if (token.length() <= 1) continue;      // remove noise

                if (topWords.contains(token)) {
                    words.add(token);
                }
            }

            for (int i = 0; i < words.size(); i++) {

                String w1 = words.get(i);
                MapWritable stripe = new MapWritable();

                for (int j = Math.max(0, i - distance);
                     j <= Math.min(words.size() - 1, i + distance); j++) {

                    if (i == j) continue;

                    String w2 = words.get(j);
                    Text neighbor = new Text(w2);

                    if (stripe.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) stripe.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighbor, new IntWritable(1));
                    }
                }

                if (!stripe.isEmpty()) {
                    context.write(new Text(w1), stripe);
                }
            }
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

        if (args.length != 4) {
            System.err.println("Usage: <input> <output> <top50> <distance>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // 🔥 LOCAL MODE FIXES
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        conf.setInt("d", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Stripes Local");

        job.setJarByClass(Problem1c_Stripes_Local.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        // 🔥 IMPORTANT OPTIMIZATION
        job.setCombinerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        // 🔥 LOCAL CACHE FIX
        job.addCacheFile(new URI("file://" + args[2]));

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}