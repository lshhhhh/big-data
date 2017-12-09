import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Multiplication1_1 {

	// Complete the Matrix1_1_Mapper class. 
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	
	// Optional, you can add and use new methods in this class
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	
	public static class Matrix1_1_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
        private String key = null;
		private String value = null;
		
		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}
				
		// Definitely, parameter type and name (Text matrix, Text entry, Context context) must not be modified
		public void map(Text matrix, Text entry, Context context) throws IOException, InterruptedException {
			// Implement map function.
            StringTokenizer itr = new StringTokenizer(entry.toString(), ",");
            String matrix_name = matrix.toString();
            String i = itr.nextToken();
            String j = itr.nextToken();
            String element_value = itr.nextToken();
            
            if (matrix_name.equals("a")) {
                value = matrix_name + "," + j + "," + element_value;
                for (int m = 0; m < n_second_cols; m++) {
                    key = i + "," + Integer.toString(m);
                    context.write(new Text(key), new Text(value));
                }
            }
            else if (matrix_name.equals("b")) {
                value = matrix_name + "," + i + "," + element_value;
                for (int n = 0; n < n_first_rows; n++) {
                    key = Integer.toString(n) + "," + j;
                    context.write(new Text(key), new Text(value));
                }
            }
		}
	}

	// Complete the Matrix1_1_Reducer class. 	
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output 
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_1_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
		
		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}
		
		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be modified
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text key, Iterable<Text> entryComponents, Context context) throws IOException, InterruptedException {
			// Implement reduce function.
            int[] A = new int[n_first_cols];
            int[] B = new int[n_first_cols];
            
            String matrix_name = null;
            int index = 0;
            int result = 0;
            
            for (Text entry : entryComponents) {
                StringTokenizer itr = new StringTokenizer(entry.toString(), ",");
                matrix_name = itr.nextToken();
                index = Integer.parseInt(itr.nextToken()); 
                if (matrix_name.equals("a"))
                    A[index] = Integer.parseInt(itr.nextToken());
                else if (matrix_name.equals("b"))
                    B[index] = Integer.parseInt(itr.nextToken());
            }

            for (int i = 0; i < n_first_cols; i++)
                result += A[i] * B[i];

            context.write(key, new Text(Integer.toString(result)));
		}
	}
	
	// Definitely, Main function must not be modified 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication1_1");

		job.setJarByClass(Multiplication1_1.class);
		job.setMapperClass(Matrix1_1_Mapper.class);
		job.setReducerClass(Matrix1_1_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
