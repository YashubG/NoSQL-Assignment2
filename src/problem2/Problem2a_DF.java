package src.problem2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import src.inputformat.CustomFileInputFormat;

public class Problem2a_DF {

	private static final String STOPWORDS_PATH_CONF = "problem2a.stopwords.path";
	private static final int TOP_N = 100;
	private static final Pattern WORD_PATTERN = Pattern.compile("[a-z]+");

	public static class DocumentFrequencyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final PorterStemmer stemmer = new PorterStemmer();
		private final Set<String> stopwords = new HashSet<String>();
		private final Text term = new Text();
		private final Text documentId = new Text();

		@Override
		protected void setup(Context context) throws IOException {
			loadStopwords(context);

			if (context.getInputSplit() instanceof FileSplit) {
				FileSplit split = (FileSplit) context.getInputSplit();
				documentId.set(split.getPath().toString());
			} else {
				documentId.set(context.getInputSplit().toString());
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> termsInRecord = new HashSet<String>();
			Matcher matcher = WORD_PATTERN.matcher(value.toString().toLowerCase(Locale.ENGLISH));

			while (matcher.find()) {
				String rawToken = matcher.group();
				if (stopwords.contains(rawToken)) {
					continue;
				}

				String stemmedToken = stemmer.stem(rawToken).toString().toLowerCase(Locale.ENGLISH);
				if (stemmedToken.length() == 0 || stopwords.contains(stemmedToken)) {
					continue;
				}

				termsInRecord.add(stemmedToken);
			}

			for (String seenTerm : termsInRecord) {
				term.set(seenTerm);
				context.write(term, documentId);
			}
		}

		private void loadStopwords(Context context) throws IOException {
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles != null) {
				for (URI cacheFile : cacheFiles) {
					File localizedFile = new File(new Path(cacheFile.getPath()).getName());
					if (localizedFile.exists()) {
						readStopwordFile(localizedFile);
					} else {
						File directFile = new File(cacheFile.getPath());
						if (directFile.exists()) {
							readStopwordFile(directFile);
						}
					}
				}
			}

			String configuredPath = context.getConfiguration().get(STOPWORDS_PATH_CONF);
			if (stopwords.isEmpty() && configuredPath != null) {
				readStopwordsFromPath(configuredPath, context.getConfiguration());
			}
		}

		private void readStopwordFile(File file) throws IOException {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			try {
				String line;
				while ((line = reader.readLine()) != null) {
					addStopword(line);
				}
			} finally {
				reader.close();
			}
		}

		private void readStopwordsFromPath(String fileName, Configuration configuration) throws IOException {
			Path path = new Path(fileName);
			FileSystem fileSystem = path.getFileSystem(configuration);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(fileSystem.open(path), StandardCharsets.UTF_8));
			try {
				String line;
				while ((line = reader.readLine()) != null) {
					addStopword(line);
				}
			} finally {
				reader.close();
			}
		}

		private void addStopword(String word) {
			String normalized = word.trim().toLowerCase(Locale.ENGLISH);
			if (normalized.length() == 0) {
				return;
			}

			Matcher matcher = WORD_PATTERN.matcher(normalized);
			while (matcher.find()) {
				String token = matcher.group();
				stopwords.add(token);
				stopwords.add(stemmer.stem(token).toString().toLowerCase(Locale.ENGLISH));
			}
		}
	}

	public static class DocumentFrequencyReducer extends Reducer<Text, Text, Text, IntWritable> {
		private final IntWritable documentFrequency = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> documents = new HashSet<String>();
			for (Text document : values) {
				documents.add(document.toString());
			}

			documentFrequency.set(documents.size());
			context.write(key, documentFrequency);
		}
	}

	public static class TopTermsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private final PriorityQueue<TermDocumentFrequency> topTerms =
				new PriorityQueue<TermDocumentFrequency>();
		private final Text outputValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) {
			TermDocumentFrequency termDocumentFrequency = parseTermDocumentFrequency(value.toString());
			if (termDocumentFrequency != null) {
				offerTopTerm(topTerms, termDocumentFrequency);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (TermDocumentFrequency termDocumentFrequency : topTerms) {
				outputValue.set(termDocumentFrequency.term + "\t" + termDocumentFrequency.documentFrequency);
				context.write(NullWritable.get(), outputValue);
			}
		}
	}

	public static class TopTermsReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
		private final PriorityQueue<TermDocumentFrequency> topTerms =
				new PriorityQueue<TermDocumentFrequency>();
		private final IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
			for (Text value : values) {
				TermDocumentFrequency termDocumentFrequency = parseTermDocumentFrequency(value.toString());
				if (termDocumentFrequency != null) {
					offerTopTerm(topTerms, termDocumentFrequency);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			List<TermDocumentFrequency> sortedTopTerms = new ArrayList<TermDocumentFrequency>(topTerms);
			sortedTopTerms.sort((left, right) -> -compareRank(left, right));

			for (TermDocumentFrequency termDocumentFrequency : sortedTopTerms) {
				outputValue.set(termDocumentFrequency.documentFrequency);
				context.write(new Text(termDocumentFrequency.term), outputValue);
			}
		}
	}

	private static void offerTopTerm(PriorityQueue<TermDocumentFrequency> topTerms,
			TermDocumentFrequency candidate) {
		if (topTerms.size() < TOP_N) {
			topTerms.offer(candidate);
			return;
		}

		TermDocumentFrequency currentWorst = topTerms.peek();
		if (currentWorst != null && compareRank(candidate, currentWorst) > 0) {
			topTerms.poll();
			topTerms.offer(candidate);
		}
	}

	private static TermDocumentFrequency parseTermDocumentFrequency(String line) {
		String[] parts = line.trim().split("\\t");
		if (parts.length != 2) {
			return null;
		}

		try {
			return new TermDocumentFrequency(parts[0], Integer.parseInt(parts[1]));
		} catch (NumberFormatException ignored) {
			return null;
		}
	}

	private static int compareRank(TermDocumentFrequency left, TermDocumentFrequency right) {
		int frequencyComparison = Integer.compare(left.documentFrequency, right.documentFrequency);
		if (frequencyComparison != 0) {
			return frequencyComparison;
		}

		return right.term.compareTo(left.term);
	}

	private static class TermDocumentFrequency implements Comparable<TermDocumentFrequency> {
		private final String term;
		private final int documentFrequency;

		private TermDocumentFrequency(String term, int documentFrequency) {
			this.term = term;
			this.documentFrequency = documentFrequency;
		}

		@Override
		public int compareTo(TermDocumentFrequency other) {
			return compareRank(this, other);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Problem2a_DF <input> <df-output> <top100-output> <stopwords-file>");
			System.exit(2);
		}

		String inputPath = otherArgs[0];
		String dfOutputPath = otherArgs[1];
		String top100OutputPath = otherArgs[2];
		String stopwordsPath = otherArgs[3];

		configuration.set(STOPWORDS_PATH_CONF, stopwordsPath);

		Job dfJob = Job.getInstance(configuration, "problem2a-document-frequency");
		dfJob.setJarByClass(Problem2a_DF.class);
		dfJob.addCacheFile(new Path(stopwordsPath).toUri());

		dfJob.setMapperClass(DocumentFrequencyMapper.class);
		dfJob.setReducerClass(DocumentFrequencyReducer.class);
		dfJob.setInputFormatClass(CustomFileInputFormat.class);
		dfJob.setOutputFormatClass(TextOutputFormat.class);
		dfJob.setNumReduceTasks(1);

		dfJob.setMapOutputKeyClass(Text.class);
		dfJob.setMapOutputValueClass(Text.class);
		dfJob.setOutputKeyClass(Text.class);
		dfJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(dfJob, new Path(inputPath));
		FileInputFormat.setInputDirRecursive(dfJob, true);
		FileOutputFormat.setOutputPath(dfJob, new Path(dfOutputPath));

		if (!dfJob.waitForCompletion(true)) {
			System.exit(1);
		}

		Job top100Job = Job.getInstance(configuration, "problem2a-top-100-document-frequency");
		top100Job.setJarByClass(Problem2a_DF.class);
		top100Job.setMapperClass(TopTermsMapper.class);
		top100Job.setReducerClass(TopTermsReducer.class);
		top100Job.setInputFormatClass(TextInputFormat.class);
		top100Job.setOutputFormatClass(TextOutputFormat.class);
		top100Job.setNumReduceTasks(1);

		top100Job.setMapOutputKeyClass(NullWritable.class);
		top100Job.setMapOutputValueClass(Text.class);
		top100Job.setOutputKeyClass(Text.class);
		top100Job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(top100Job, new Path(dfOutputPath));
		FileOutputFormat.setOutputPath(top100Job, new Path(top100OutputPath));

		System.exit(top100Job.waitForCompletion(true) ? 0 : 1);
	}
}
