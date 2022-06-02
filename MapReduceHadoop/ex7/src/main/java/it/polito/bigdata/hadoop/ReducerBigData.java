package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	StringBuilder info = new StringBuilder();
    	
    	for (Text sentence : values) {
    		if(info.length() == 0) {
    			info.append(sentence.toString());
    		} else {
    			info.append(","+sentence.toString());
    		}
    	}
    	context.write(key, new Text(info.toString()));
    }
}
