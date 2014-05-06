import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Text, IntWritable, Text, IntWritable
import Utils.Utils;

public class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	  @Override
	  protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
	    org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text>.Context context)
	       throws IOException, InterruptedException {
	     
		 Configuration conf = context.getConfiguration();
	         
		 double newPR=0;
		 double oldPR=0;
		 String dest_list="";
		 double sumOfEdgeVals=0;
		 
		 int N = conf.getInt("N", -1); //total number of nodes
		 if (N==-1){
			 Utils.log("error: value of N not passed to reducer");
		 }
		 
		 for (Text value : values) {
			 String val = value.toString();
			 
			 //(srcID, PR)
			 if(val.charAt(0)=='b'){
				 oldPR = Utils.str2double(val.substring(1));
			 }
			 //(srcID,   dest_list)
			 else if(val.charAt(0)=='o'){
				 dest_list = val.substring(1);
			 }
			 //(dist_ID,  srcPR/edgeVal)
			 else if(val.charAt(0)=='g'){
				 sumOfEdgeVals+=Utils.str2double(val.substring(1));
			 }
			 else{
				 //error
				 Utils.log("error: reducer got some unexpected value.charAt(0)");
			 }
	     }
		 
		 newPR = 0.15/N + 0.85 * sumOfEdgeVals;
		 
		 //convergence TODO: check this again
		 double v = Math.abs(oldPR-newPR)/newPR;
		 float counter = conf.getFloat("counter", -1);
         if(counter==-1){Utils.log("error: read counter value error in reduce");}
		 conf.setFloat("counter", (float) (counter+v));
         
		 //
		 String value = Utils.double2str(newPR)+ (dest_list.isEmpty()?"":(","+dest_list));
		 
	     context.write(key, new Text(value));
	  }
	}