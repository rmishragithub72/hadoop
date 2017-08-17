package join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		//001,1234,C001,5,<date>
		String myKey = value.toString().split(",")[1];
		String myVal = "Order,".concat(value.toString());
		context.write(new Text(myKey), new Text(myVal));
	}

}
