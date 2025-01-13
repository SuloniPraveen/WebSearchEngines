import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bigram {

    public static class BigramMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text docId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] token = line.split("\t", 2);

            docId.set(token[0]);
            String[] words = token[1].toLowerCase().replaceAll("[^a-z]+", " ").split(" ");

            for (int index = 0; index < words.length - 1; index++) {
                String firstWord = words[index];
                String followingWord = words[index + 1];
                String bigramWord = firstWord + " " + followingWord;

                if (matchesBigram(bigramWord)) {
                    context.write(new Text(bigramWord), docId);
                }
            }
        }

        private boolean matchesBigram(String bigram) {
            return bigram.equals("computer science") ||
                   bigram.equals("information retrieval") ||
                   bigram.equals("power politics") ||
                   bigram.equals("los angeles") ||
                   bigram.equals("bruce willis");
        }
    }

    public static class BigramReducer extends Reducer<Text, Text, Text, Text> {

        private final Text frequencyInfo = new Text();

        @Override
        public void reduce(Text biword, Iterable<Text> docIds, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> frequencyMap = new HashMap<>();

            for (Text docId : docIds) {
                String doc = docId.toString();
                frequencyMap.put(doc, frequencyMap.getOrDefault(doc, 0) + 1);
            }

            StringBuilder frequencies = new StringBuilder();
            for (String document : frequencyMap.keySet()) {
                frequencies.append(document).append(":").append(frequencyMap.get(document)).append(" ");
            }

            frequencyInfo.set(frequencies.toString().trim());
            context.write(biword, frequencyInfo);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job bigramJob = Job.getInstance(conf, "Bigram Frequency Count");

        bigramJob.setJarByClass(Bigram.class);
        FileInputFormat.addInputPath(bigramJob, new Path("in/devdata"));
        FileOutputFormat.setOutputPath(bigramJob, new Path(args[1]));

        bigramJob.setMapperClass(BigramMapper.class);
        bigramJob.setReducerClass(BigramReducer.class);

        bigramJob.setOutputKeyClass(Text.class);
        bigramJob.setOutputValueClass(Text.class);

        System.exit(bigramJob.waitForCompletion(true) ? 0 : 1);
    }
}
