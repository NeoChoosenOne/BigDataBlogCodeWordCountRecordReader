package com.darchbps.wordcountbloginput;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
/*
 * Class to Read Not Structured Files extract text and make it a word count execution.
 */
public class NSFExecutor{
	final static Logger logger = Logger.getLogger(NSFExecutor.class);

	/*Class extends from Mapper Class Input params (Object, Text) usually Object Key is the offset of bytes 
	   * and Text are the lines at input text, Output params (Text, IntWriteable).
	   */

	  public static class Map extends Mapper<Object, Text, Text, IntWritable>{
		  
		//Instance an IntWritable Object with value 1 to set it to all words in input text.
	    private final static IntWritable one = new IntWritable(1);
	    //Instance an empty Text Object to keep into this wrapper word by word to send it to reducer class.
	    private Text word = new Text();
	    //Method implementation for map function, input params (key,value) as types (Object, Text) and ouput values as context as type Context.
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      //Object that receives line by line from input text from corresponding node to split it by space, 
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      //loop to iterate word by word from the previous line splitted.
	      while (itr.hasMoreTokens()) {
	    	//Add to word wrapper object the word at the loop.
	        word.set(itr.nextToken());
	        //Send the output of Map (text, IntWritable)
	        context.write(word, one);
	      }
	    }
	  }
	  /*Class extends from Reducer Class input 
	   * 
	   */
	  public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	    //Instance an IntWritable Object with empty value to make the sum of all the word in the input key.
	    private IntWritable result = new IntWritable();
	    //Method Implementation for map function, input params (key, Iterable<IntWritable>) where Iterable<IntWritable> have all the ones for the input key.
	    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      //loop to get all the values for this input key
	      for (IntWritable val : values) {
	    	//sum the values for the input key
	        sum += val.get();
	      }
	      //Add to result wrapper the total sum for all the values at input key. 
	      result.set(sum);
	      //Send the output of Reducer (Text, IntWritable)
	      context.write(key, result);
	    }
	  }
	  //This example takes a custom in input Input split.
	  public static void main(String[] args) throws Exception {
		 
		 //Hadoop configuration 
		 Configuration conf = new Configuration();
		 //Job Configuration set it a hadoop conf and Job name.
		 Job job = Job.getInstance(conf,"NSFWordCount");
		 //Set to job configuration the main class that contains main method.
		 job.setJarByClass(NSFExecutor.class);
		 
		 job.setInputFormatClass(NSFInputFormat.class);
		 job.setOutputValueClass(NSFInputFormat.class);
		 //Set to job configuration the class where the Mapper Implementation is.
		 job.setMapperClass(Map.class);
		 //Set to job configuration the class where the Combiner Implementation is.
		 job.setCombinerClass(Reduce.class);
		 //Set to job configuration the class where the Reducer Implementation is.
		 job.setReducerClass(Reduce.class);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 //Input path in HDFS to read files to InputSpliter and Record Reader 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 //Output path in HDFS to put output result for this job
		 FileOutputFormat.setOutputPath(job,new Path(args[1]));
		 //Wait until Job workflow finish.
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
