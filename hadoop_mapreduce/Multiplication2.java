import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Multiplication2 {
	// Complete the Matrix2_Mapper class. 
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class

	public static class Matrix2_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		int result_cols = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
        private String key = null;
		private String value = null;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			result_cols = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameter type and name (Text matrix, Text entry, Context context) must not be modified
		public void map(Text matrix, Text entry, Context context) throws IOException, InterruptedException {
			// Implement map function.
            StringTokenizer itr = new StringTokenizer(entry.toString(), ",");
            String matrix_name = matrix.toString();
            
            if (matrix_name.equals("a")) {
                String i = itr.nextToken();
                String k = itr.nextToken();
                String element_value = itr.nextToken();
                for (int l = 0; l < n_second_cols; l++) {
                    for (int j = 0; j < result_rows; j++) {
                        key = i + "," + Integer.toString(j);
                        value = matrix_name + "," + k + "," + Integer.toString(l) + "," + element_value;
                        context.write(new Text(key), new Text(value));
                    }
                }
            }
            else if (matrix_name.equals("b")) {
                String k = itr.nextToken();
                String l = itr.nextToken();
                String element_value = itr.nextToken();
                for (int i = 0; i < result_cols; i++) {
                    for (int j = 0; j < result_rows; j++) {
                        key = Integer.toString(i) + "," + Integer.toString(j);
                        value = matrix_name + "," + k + "," + l + "," + element_value;
                        context.write(new Text(key), new Text(value));
                    }
                }
            }
            else if (matrix_name.equals("c")) {
                String l = itr.nextToken();
                String j = itr.nextToken();
                String element_value = itr.nextToken();
                for (int i = 0; i < result_cols; i++) {
                    for (int k = 0; k < n_first_cols; k++) {
                        key = Integer.toString(i) + "," + j;
                        value = matrix_name + "," + Integer.toString(k) + "," + l + "," + element_value;
                        context.write(new Text(key), new Text(value));
                    }
                }
            }
		}
	}


	// Complete the Matrix2_Reducer class. 	
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output 
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix2_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		int result_columns = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
        private String matrix_name = null;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			result_columns = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be modified		
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text key, Iterable<Text> entryComponents, Context context) throws IOException, InterruptedException {
			// Implement reduce function.
            int[] A = new int[n_first_cols];
            int[][] B = new int[n_first_cols][n_second_cols];
            int[] C = new int[n_second_cols];
            int result = 0;
            
            StringTokenizer itr1 = new StringTokenizer(key.toString(), ",");
            int i = Integer.parseInt(itr1.nextToken());
            int j = Integer.parseInt(itr1.nextToken());

            for (Text entry : entryComponents) {
                StringTokenizer itr = new StringTokenizer(entry.toString(), ",");
                matrix_name = itr.nextToken();
                int k = Integer.parseInt(itr.nextToken());
                int l = Integer.parseInt(itr.nextToken());
                if (matrix_name.equals("a")) {
                    A[k] = Integer.parseInt(itr.nextToken());
                }
                else if (matrix_name.equals("b")) {
                    B[k][l] = Integer.parseInt(itr.nextToken());
                }
                else if (matrix_name.equals("c")) {
                    C[l] = Integer.parseInt(itr.nextToken());
                }
            }

            for (int k = 0; k < n_first_cols; k++) {
                for (int l = 0; l < n_second_cols; l++) {
                    result += A[k] * B[k][l] * C[l];
                }
            }

            context.write(key, new Text(Integer.toString(result)));
		}
	}

	// Definitely, Main function must not be modified 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication");

		job.setJarByClass(Multiplication2.class);
		job.setMapperClass(Matrix2_Mapper.class);
		job.setReducerClass(Matrix2_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));
		job.getConfiguration().setInt("n_third_cols", Integer.parseInt(args[5]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
