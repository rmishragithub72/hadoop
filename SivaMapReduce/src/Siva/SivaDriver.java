package Siva;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SivaDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("The main method is invoked");
		try {
			Job job = new Job(new Configuration());
			job.setJobName("SivaMapReduce");
			job.setJarByClass(SivaDriver.class);
			job.setMapperClass(Siva.SivaMapper.class);
			job.setReducerClass(Siva.SivaReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			int reduceTasknum = Integer.valueOf(args[2]);
			if (reduceTasknum >= 0)
				job.setNumReduceTasks(reduceTasknum);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.submit();
			job.waitForCompletion(true);
			System.out.println("Job completed successfully");
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
