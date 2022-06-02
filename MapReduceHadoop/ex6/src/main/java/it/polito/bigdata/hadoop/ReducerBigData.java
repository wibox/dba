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
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	double local_min = Double.MAX_VALUE;
    	double local_max = Double.MIN_VALUE;
    	
    	for (FloatWritable pm10 : values) {
    		if (pm10.get() > local_max) {
    			local_max = pm10.get();
    		}
    		if (pm10.get() < local_min) {
    			local_min = pm10.get();
    		}
    	}
    	context.write(key, new Text("max="+local_max+"_min="+local_min));
    }
}
