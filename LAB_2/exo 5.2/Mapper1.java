package pasquiou.taitai.lab2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;


//MapReduce iterative mapper for pageranking
//
//------
// * Mapper: --> input  : (byteCount, line)
//           --> output : (page, page)


public class Mapper1
    extends Mapper<LongWritable, Text, Text, Text> {
    
        private static String page1 = new String();
        private static String page2 = new String();
        public static Set<String> NODES = new HashSet<String>();
 
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
    
            String line     = value.toString();
            String[] urls   = line.split("\\t");         
            
            if (!NODES.contains(page1)) {
                NODES.add(page1);
            }
            else if (!NODES.contains(page2)) {
                NODES.add(page2);
            }
            
            page1    = urls[0];
            page2    = urls[1];

            context.write(new Text(page1), new Text(page2));
            context.write(new Text(page2), new Text(page1));
 
        }
    }