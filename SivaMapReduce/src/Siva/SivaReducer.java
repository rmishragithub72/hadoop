package Siva;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SivaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> listCount,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(key, listCount, context);
		int count =0;
		while(listCount.iterator().hasNext()){
			count+=listCount.iterator().next().get();
		}
		context.write(key, new IntWritable(count));
	}

}
