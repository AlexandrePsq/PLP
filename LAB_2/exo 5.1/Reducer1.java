package pasquiou.taitai.lab2.exercice5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;


//MapReduce First part of the TF-IDF
//
// *** Round 1 ***
//------
// * Reducer: --> input  : (word@doc, [count, count, ...])
//            --> output : (word@doc, n)


public class Reducer1
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
