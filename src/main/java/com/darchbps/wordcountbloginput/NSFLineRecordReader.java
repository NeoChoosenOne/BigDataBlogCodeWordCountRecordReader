package com.darchbps.wordcountbloginput;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cbds.readnotstructuredfiles.FileExtraction;

//The RecordReader Class here is where the actual file information is read and parsed. We will implement this by making use of the RecordReader

public class NSFLineRecordReader extends RecordReader<Text, Text> {

	private Text key = new Text();
	private Text value = new Text();
	private int currentLine = 0;
	private List<String> lines = null;

    String contenido = null;
    
    //Abstract Method that need to implement from RecordReader Class this method is where the actual file information is read and parsed to send it to mapper.
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException{
		
	    FileSplit fileSplit = (FileSplit) split;
	    final Path file = fileSplit.getPath();
	
	    //Hadoop configuration
	    Configuration conf = context.getConfiguration();
	    //File System object to manipulated HDFS files reciving a hadoop configuration.
	    FileSystem fs = file.getFileSystem(conf);
	    //Object to manipulated files to get the input Stream of a file in HDFS.
	    FSDataInputStream filein = fs.open(fileSplit.getPath());
	    //conditional sentence to check if filein is null or not.
	    if (filein != null) {
	    	//Array object to get name and extension of a file extracted in HDFS.
		    String [] splits = file.getName().split("\\.");
		    //New String object that points to splits[1] memory value.
		    String tipo = splits[1];
		    //Instance of a FileExtraction Class that make extration of text from files in HDFS (doc,docx,ppt,pptx,xls,xlsx,pdf).
		    FileExtraction fe = new FileExtraction();
		    //Try statement to catch any exception that fe object can throw out.
		    try {
				lines = fe.fileExtraction(filein, tipo);
	            currentLine = 0;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}
	
	//Over write a method that set values to (key,value) pair and False ends the reading process
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		//conditional sentence to check if the key is null
	    if (key == null) {
	    	//if key is null, we Instantiate key as Text
	        key = new Text();
	    }
	    
		//conditional sentence to check if the value is null
	    if (value == null) {
	    	//if value is null, we Instantiate value as Text
	        value = new Text();
	    }
	    //conditional sentence to check if the list have more elements to read
	    if (currentLine < lines.size()) {
	    	
	    	//String Object that poitns to current lines's element
	        String line = lines.get(currentLine);
	        //Set value to object key
	        key.set("");
	        //Set value to object key
	        value.set(line);
	        //increment one the currentLine.
	        currentLine++;
	        return true;
	    } else {
	        //If all the elements were read the (key,value) pair set to null again to finish the process.
	        key = null;
	        value = null;
	        return false;
	    }
	}
	//Abstract method to get the current value of Key.
	public Text getCurrentKey() throws IOException, InterruptedException {
	    return key;
	}
	//Abstract method to get the current value of value.
	public Text getCurrentValue() throws IOException, InterruptedException {
	    return value;
	}
	//Abstract method to get the process of RecordReader.
	public float getProgress() throws IOException, InterruptedException {
	    return (100.0f / lines.size() * currentLine) / 100.0f;
	}
	
	//Abstract method to close the RecordReader process.
	public void close() throws IOException {

	}
}
