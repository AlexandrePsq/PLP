package pasquiou.taitai.lab2.exercice53;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

//* Mapper: --> input  : (byteCount, line)
//			--> output : (height_espece, 1)


public class MapperHeight
extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static       Text   type = new Text();


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

    	
        String line    = value.toString().toLowerCase();
        String[] info = line.split(";");        
        if(info[6] != null ) {
        	try {
        		Double height = Double.parseDouble(info[6]);       
                String espece = info[3]; //on suppose que le type de l'arbre correspond a son espece 
            	type.set(espece);
                context.write(type, new DoubleWritable(height));
        	} catch(Exception e) {
        		context.write(type, new DoubleWritable(0));
        	}
        	
        }
        }
}

