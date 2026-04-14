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

public class Problem1b_Pairs_IMC_Local {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, Integer> localMap = new HashMap<>();
        private Set<String> topWords = new HashSet<>();
        private int distance;

        private static final int FLUSH_LIMIT = 10000;

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
                for (int j = Math.max(0,i-distance); j <= Math.min(words.size()-1,i+distance); j++) {

                    if (i == j) continue;

                    String pair = words.get(i) + "," + words.get(j);
                    localMap.put(pair, localMap.getOrDefault(pair, 0) + 1);

                    if (localMap.size() > FLUSH_LIMIT) {
                        flush(context);
                        localMap.clear();
                    }
                }
            }
        }

        private void flush(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String,Integer> e : localMap.entrySet()) {
                context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            flush(context);
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("d", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Pairs IMC Local");

        job.setJarByClass(Problem1b_Pairs_IMC_Local.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI("file://" + args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}