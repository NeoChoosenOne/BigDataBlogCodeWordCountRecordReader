package com.darchbps.wordcountbloginput;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//Class that defines Input Format that extends from FileInputFormat class.
public class NSFInputFormat extends FileInputFormat<Text, Text> {

	  //Method that is required to implement from FileInputFormat Class.
	  public RecordReader<Text, Text> createRecordReader(InputSplit split,
	    TaskAttemptContext context) throws IOException, InterruptedException {
		  //Return 
	      return new NSFLineRecordReader();
	  }

	  // Do not allow to ever split PDF files, even if larger than HDFS block size
	  protected boolean isSplitable(JobContext context, Path filename) {
	    return false;
	  }

	}