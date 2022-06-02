package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] words = value.toString().split("\\s+");
    	String cleanWord;
    	
    	for (String word : words) {
    		if(word.compareTo("or")!=0 && word.compareTo("and")!=0 && word.compareTo("not")!=0) {
    			cleanWord = word.toLowerCase();
    			context.write(new Text(cleanWord), new Text(key));
    		}
    	}
    	
    }
}
