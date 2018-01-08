package pasquiou.taitai.lab2.exercice53;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

//* Mapper: --> input  : (byteCount, line)
//			--> output : (espece, 1)


public class MapperType
extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static       Text   type = new Text();


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line    = value.toString().toLowerCase();
        String[] info = line.split(";");
        //String geoloc = info[0];
        String espece = info[3]; //on suppose que le type de l'arbre correspond a son espece


            type.set(espece);
            context.write(type, one);
        }
}

