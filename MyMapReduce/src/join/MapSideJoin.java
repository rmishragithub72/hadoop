package join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

	public static void main(String[] args) {
		try {
			Job job = new Job(new Configuration());
			job.setJobName("My Map Side Join");
			job.setJarByClass(MapSideJoin.class);
			
			//set reducer
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			//set mapper for first fill
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ProductMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);
			
			//set output format and path
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setNumReduceTasks(1);
			System.exit(job.waitForCompletion(true)? 0 : 1);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
