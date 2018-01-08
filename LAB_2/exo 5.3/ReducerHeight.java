package pasquiou.taitai.lab2.exercice53;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;



// * Reducer: --> input  : (espece, 1)
//            --> output : (espece, especeCount)


public class ReducerHeight
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 
        private static DoubleWritable max_height = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
 
            Double max= 0.00;
            
            for (DoubleWritable val : values) {
                if(max < val.get()){
                	max = val.get();
                }
            }
            max_height.set(max);
            context.write(key, max_height);
        }
    }
