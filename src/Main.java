import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	
	public static void main(String[] args) throws Exception {		

		long start = new Date().getTime();		
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		Job job2 = Job.getInstance();
		Job job3 = Job.getInstance();
		
		
		
		try{
	    	 
			//First Job
	    	 job.setJarByClass(Operation1.class);
	    	 System.out.println("\nParsing Input Start**********\n");
	    	 System.out.println("********************************");
			
	    	 job.setMapperClass(Operation1.Map1.class);
	//    	 job.setCombinerClass(Operation1.Reduce1.class);
	    	 job.setReducerClass(Operation1.Reduce1.class);
			
	    	 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);
			
			 job.setInputFormatClass(TextInputFormat.class);
			
			 
			 //Second JOB

			 job2.setJarByClass(Operation2.class);
			 job2.setMapperClass(Operation2.Map2.class);
			 job2.setReducerClass(Operation2.Reduce2.class);
				
			 job2.setOutputKeyClass(Text.class);
			 job2.setOutputValueClass(Text.class);
				
			 job2.setInputFormatClass(TextInputFormat.class);
			 

			 //Third Job
			 job3.setJarByClass(FinalOperation.class);
			 job3.setMapperClass(FinalOperation.Map3.class);
			 job3.setReducerClass(FinalOperation.Reduce3.class);
				
			 job3.setOutputKeyClass(Text.class);
			 job3.setOutputValueClass(Text.class);
				
			 job3.setInputFormatClass(TextInputFormat.class);
			 
		     FileInputFormat.addInputPath(job, new Path(args[0]));			 
		     FileOutputFormat.setOutputPath(job, new Path(args[1]+"Output_1"));
		     
		     FileInputFormat.addInputPath(job2, new Path(args[1]+"Output_1"));
		     FileOutputFormat.setOutputPath(job2, new Path(args[1]+"Output_2"));
										
		     FileInputFormat.addInputPath(job3, new Path(args[1]+"Output_2"));
		     FileOutputFormat.setOutputPath(job3, new Path(args[1]));
	    	 
	     }catch(Exception e)
	     {
	    	 System.out.println("Error is"+e);
	    	 e.printStackTrace();
	     }	
		
			
	    // job.setJarByClass(Main.class);
	     
	     job.waitForCompletion(true);
	     job2.waitForCompletion(true);
	//     job3.waitForCompletion(true);

	     boolean status = job3.waitForCompletion(true);
	   
	     if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nStock Analysis Job took " + (end-start)/1000 + "seconds\n");
			
	     }
		System.out.println("\n**********Stock Votality-> End**********\n");		
		
	}//End of main Function
	
	
		
	
		

	
}//End of Main.class