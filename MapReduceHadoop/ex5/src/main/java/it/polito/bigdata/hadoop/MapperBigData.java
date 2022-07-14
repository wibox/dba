package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] fields = value.toString().split(",");
    	String sensorId = fields[0];
    	float PM10 = Float.parseFloat(fields[2]);
    	
    	context.write(new Text(sensorId), new FloatWritable(new Float(PM10)));
    	
    }
}
