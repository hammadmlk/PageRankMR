import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Utils.Utils;


public class PageRank {
	
	public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		   
		   //delete old files
		   Utils.log("Deleting Old Files");
		   
		   Path path1 = new Path(args[1]); 
		   Path path2 = new Path(args[2]);
		   FileSystem fs = FileSystem.get(path1.toUri(), conf); 
		   fs.delete(path1, true);
		   fs.delete(path2, true);
		   Utils.log("Done deleting old files");
		   
		   
		   
		   //comma separate key value in output file.
		   conf.set("mapred.textoutputformat.separator", ","); //Prior to Hadoop 2 (YARN)
           conf.set("mapreduce.textoutputformat.separator", ",");  //Hadoop v2+ (YARN)
           conf.set("mapreduce.output.textoutputformat.separator", ",");
           conf.set("mapreduce.output.key.field.separator", ",");
           conf.set("mapred.textoutputformat.separatorText", ","); // ?
		   
           //set a global float called counter
           conf.setFloat("counter", 0);
           
           //set a global Integer called N, number of nodes
           conf.setInt("N", 10); //TODO: give real N
           
           
           
           /*//////////
           ////////// PREPROCESS JOB  */
           Utils.log("Starting Preprocessing");
		   
           Job job1 = new Job(conf, "preprocessing");
		   
		   job1.setJarByClass(PageRank.class); // not sure

		   job1.setOutputKeyClass(IntWritable.class);
		   job1.setOutputValueClass(Text.class);
		 
		   job1.setMapperClass(PreProcessMap.class);
		   job1.setReducerClass(PreProcessReduce.class);
		 
		   job1.setInputFormatClass(TextInputFormat.class);
		   job1.setOutputFormatClass(TextOutputFormat.class);
		 
		   FileInputFormat.addInputPath(job1, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		 
		   job1.waitForCompletion(true); //blocking
		   Utils.log("Done Preprocessing");
		   
           /*////////
           /////////*/
           
		   Utils.log("Starting PageRank Iter1");
		   
           
           //make a new mapreduce job called pagerank
		   Job job = new Job(conf, "pagerank");
		   
		   job.setJarByClass(PageRank.class);

		   job.setOutputKeyClass(IntWritable.class);
		   job.setOutputValueClass(Text.class);
		 
		   job.setMapperClass(Map.class);
		   job.setReducerClass(Reduce.class);
		 
		   job.setInputFormatClass(TextInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		 
		   FileInputFormat.addInputPath(job, new Path(args[1]));
		   FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		   job.waitForCompletion(true); //blocking
		   
		   Utils.log("1 Mapreduce Job Completed");
		   Utils.log("Counter value: "+conf.getFloat("counter", -1));
		   
		   Utils.log("Trying to merge files");
		   
		   //Try to merge output files
		   
		   Path srcPath = new Path(args[1]); 
           Path dstPath = new Path(args[1]+".combined"); 
           try { 	
        	   FileSystem hdfs = FileSystem.get(dstPath.toUri(), conf); 	
        	   FileUtil.copyMerge(hdfs, (srcPath), 
        			   hdfs, (dstPath), false, conf, null); 
        	   } 
           catch (IOException e) { 		
        	   //errror
    		   Utils.log("IOException when merging files");
           }
           
		   Utils.log("Merged Files. Output: "+dstPath.toString());
           
           
		   Utils.log("Now should check for convergence and re-run if required.");
           //check if converged. if not re-run
		}

}
