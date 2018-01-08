package pasquiou.taitai.lab2.exercice53;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;



// * Reducer: --> input  : (espece, 1)
//            --> output : (espece, especeCount)


public class ReducerType
    extends Reducer<Text, IntWritable, Text, IntWritable> {
 
        private static IntWritable n = new IntWritable();
 
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
 
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            n.set(sum);
            context.write(key, n);
        }
    }
