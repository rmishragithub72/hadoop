package join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		//1234,Product A,123.00,Cat-1
		String myKey = value.toString().split(",")[0];
		String myVal = "Product,".concat(value.toString());
		context.write(new Text(myKey), new Text(myVal));
	}

}
