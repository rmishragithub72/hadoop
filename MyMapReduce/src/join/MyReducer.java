package join;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context ctx) throws IOException,
			InterruptedException {
		String product = "";
		List<String> orders = new ArrayList<String>();
		while(values.iterator().hasNext()){
			String[] val = values.iterator().next().toString().split(",");
			if(val[0].equalsIgnoreCase("product")){
				product = val[2];
			}else{
				orders.add(String.format("%s,%s,%s", val[1],val[3],val[4]));
			}
		}
		
		for (String str : orders){
			ctx.write(key, new Text(String.format("%s,%s",product,str )));
		}
				
		
	}
	

}
