package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
	private float pm10_sum;
	private int count;
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	for (FloatWritable pm10 : values) {
    		pm10_sum = pm10_sum + pm10.get();
    		count = count + 1;
    	}
    	context.write(key, new FloatWritable(pm10_sum/count));
    }
}
