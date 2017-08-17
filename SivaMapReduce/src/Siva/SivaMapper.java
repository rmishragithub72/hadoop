package Siva;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SivaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException{
		for (String word : value.toString().split(" ")){
			context.write(new Text(word), new IntWritable(1));
		}
		
	}

}
