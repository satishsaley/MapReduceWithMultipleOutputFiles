package mapred.multipleoutput;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutReducer extends Reducer<IntWritable, Text, Text, Text>{

	private MultipleOutputs<Text,Text> multipleOutputs = null;
	
	@Override
	protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> ite = values.iterator();
		while (ite.hasNext()) {
			Text t = ite.next();
			Text outK = new Text(key+"");
			multipleOutputs.write(outK, t, key+"");
            context.write(outK, t);
		}
	}
	

}
