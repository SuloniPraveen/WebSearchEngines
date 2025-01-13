import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unigram {

    public static class UnigramMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text docIdentifier = new Text();
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();
            String[] parts = inputLine.split("\t", 2);
            docIdentifier.set(parts[0]);

            StringTokenizer wordIterator = new StringTokenizer(parts[1].toLowerCase().replaceAll("[^a-z]+", " "));
            while (wordIterator.hasMoreTokens()) {
                word.set(wordIterator.nextToken());
                context.write(word, docIdentifier);
            }
        }
    }

    public static class UnigramReducer extends Reducer<Text, Text, Text, Text> {

        private final Text docFrequencyInfo = new Text();

        @Override
        public void reduce(Text wordTerm, Iterable<Text> docIdentifiers, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> occurrenceMap = calculateOccurrences(docIdentifiers);
            StringBuilder frequencyBuilder = buildFrequencyList(occurrenceMap);
            docFrequencyInfo.set(frequencyBuilder.toString().trim());
            context.write(wordTerm, docFrequencyInfo);
        }

        private HashMap<String, Integer> calculateOccurrences(Iterable<Text> docIdentifiers) {
            HashMap<String, Integer> occurrenceMap = new HashMap<>();
            for (Text docId : docIdentifiers) {
                occurrenceMap.put(docId.toString(), occurrenceMap.getOrDefault(docId.toString(), 0) + 1);
            }
            return occurrenceMap;
        }

        private StringBuilder buildFrequencyList(HashMap<String, Integer> occurrenceMap) {
            StringBuilder frequencies = new StringBuilder();
            for (String doc : occurrenceMap.keySet()) {
                frequencies.append(doc).append(":").append(occurrenceMap.get(doc)).append(" ");
            }
            return frequencies;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job unigramJob = Job.getInstance(conf, "Unigram Indexing");

        unigramJob.setJarByClass(Unigram.class);
        FileInputFormat.addInputPath(unigramJob, new Path("in/fulldata"));
        FileOutputFormat.setOutputPath(unigramJob, new Path(args[1]));

        unigramJob.setMapperClass(UnigramMapper.class);
        unigramJob.setReducerClass(UnigramReducer.class);

        unigramJob.setOutputKeyClass(Text.class);
        unigramJob.setOutputValueClass(Text.class);

        System.exit(unigramJob.waitForCompletion(true) ? 0 : 1);
    }
}
