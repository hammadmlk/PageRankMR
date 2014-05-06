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
           
           
           //make a new mapreduce job called pagerank
		   Job job = new Job(conf, "pagerank");
		   
		   job.setJarByClass(PageRank.class);

		   job.setOutputKeyClass(IntWritable.class);
		   job.setOutputValueClass(Text.class);
		 
		   job.setMapperClass(Map.class);
		   job.setReducerClass(Reduce.class);
		 
		   job.setInputFormatClass(TextInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		 
		   FileInputFormat.addInputPath(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		   job.waitForCompletion(true); //blocking
		   
		   Utils.log("1 Mapreduce Job Completed");
		   Utils.log("Counter value: "+conf.getFloat("counter", -1));
		   
		   Utils.log("Trying to merge files");
		   //Try to merge output files
		   Path srcPath = new Path(args[1]); 
           Path dstPath = new Path("combined"+args[1]); 
           try { 	
        	   FileSystem hdfs = FileSystem.get(conf); 	
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
