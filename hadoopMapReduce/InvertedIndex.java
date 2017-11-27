import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*; //FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {
            
        private String word = null;
        private Text fileName = new Text();
        
        public void map(Text term, Text docID, Context context) throws IOException, InterruptedException {
            Path filePath = ((FileSplit)context.getInputSplit()).getPath();
            StringTokenizer itr = new StringTokenizer(term.toString());

            while (itr.hasMoreTokens()) {
                word = itr.nextToken().toLowerCase();
                if (word.substring(word.length()-1, word.length()).equals("."))
                    word = word.substring(0, word.length()-1);
           
                context.write(new Text(word), new Text(filePath.getName()));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    
        public void reduce(Text term, Iterable<Text> docIDs, Context context) throws IOException, InterruptedException {
            StringBuilder documents = new StringBuilder();
            boolean first = true;

            for (Text docID : docIDs) {
                if (!first)
                    documents.append(",");
                else
                    first = false;
                documents.append(docID.toString());
            }
            context.write(term, new Text(documents.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

