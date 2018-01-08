package pasquiou.taitai.lab2;


import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import pasquiou.taitai.lab2.Mapper1;

//MapReduce iterative reducer for pageranking
//
//------
// * Reducer: --> input  : (page, [page1, page2, ...])
//            --> output : (page, page1;page2;...)


public class Reducer1
    extends Reducer<Text, Text, Text, Text> {
        
        public static Double DAMPING = 0.85;
        public static Integer NodesCount = Mapper1.NODES.size();
    
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {

            
            Boolean Switch = true;
            String pages = Double.toString(DAMPING / NodesCount) + "\t";

            for (Text page:value){
                if (!Switch)
                    pages += ";";
                pages += page.toString();
                Switch = false;
            }
          
            
            context.write(key , new Text(pages));
        }
    }