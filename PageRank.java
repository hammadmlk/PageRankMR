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
		conf = new Configuration();
		Path path1 = new Path(args[1]);
		Path path1_1 = new Path(args[1]+".combined"); 
		Path path2 = new Path(args[2]);
		FileSystem fs = FileSystem.get(path1.toUri(), conf); 
		fs.delete(path1_1, true);
		fs.delete(path1, true);
		fs.delete(path2, true);
		Utils.log("Done deleting old files");
		//end
	      
		/* === PREPROCESS JOB ===  */
		Utils.log("Starting Preprocessing");
		conf = new Configuration();
        
        conf.set("mapred.textoutputformat.separator", ","); //Prior to Hadoop 2 (YARN)
        
        //set a global Integer called N, number of nodes
        conf.setInt("N", 10); //TODO: give real N
        
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
		 
		boolean b = job1.waitForCompletion(true); //blocking
		Utils.log("Done Preprocessing - "+ b );
		
		//Try to merge output files
		try{
			Utils.log("Combining pre processing files");
			Path srcPath = new Path(args[1]); 
	        Path dstPath = new Path(args[1]+".combined"); 
        
        	FileSystem hdfs = FileSystem.get(dstPath.toUri(), conf); 	
        	FileUtil.copyMerge(hdfs, (srcPath), hdfs, (dstPath), false, conf, null); 
            Utils.log("Merged Files. Output: "+dstPath.toString());
        }
        catch (IOException e) { 		
        	//errror
        	Utils.log("IOException when merging preprocessing files");
        }
		
        /*===END Preprocessing===*/   
		   
		 
        /* === Page Rank Itr1 === */   
        Utils.log("Starting PageRank Iter1");
		
        conf = new Configuration();
        //comma separate key value in output file.
        conf.set("mapred.textoutputformat.separator", ","); //Prior to Hadoop 2 (YARN)
		   
        //set a global float called counter   
        //set a global Integer called N, number of nodes
        conf.setFloat("counter", 0);
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
	 
        FileInputFormat.addInputPath(job, new Path(args[1]+".combined"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        Utils.log("Waiting for completion of PageRank Iter1");
        boolean b0 = job.waitForCompletion(true); //blocking
	   
        Utils.log("Mapreduce iter1 Completed - "+b0);
        
        Utils.log("Counter value: "+conf.getFloat("counter", -1));

        
        //Try to merge output files
        try {
	       Utils.log("Trying to merge files");
	       Path srcPathx = new Path(args[2]); 
	       Path dstPathx = new Path(args[2]+".combined"); 
         	
		   FileSystem hdfs = FileSystem.get(dstPathx.toUri(), conf); 	
		   FileUtil.copyMerge(hdfs, (srcPathx), hdfs, (dstPathx), false, conf, null); 
	       Utils.log("Merged Files. Output: "+dstPathx.toString());
	    } 
        catch (IOException e) { 		
		   //errror
		   Utils.log("IOException when merging files");
        }
	   	   	   
        Utils.log("Now should check for convergence and re-run if required.");
        
        //TODO: check if converged. if not re-run
        
        /* === END Page Rank Itr1 === */
	}

}
