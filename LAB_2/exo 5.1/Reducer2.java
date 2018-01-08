package pasquiou.taitai.lab2.exercice5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;


// * Reducer: --> input  : (doc_id, [word;n, word;n, ...])
//            --> output : (word@doc_id, n;N)


public class Reducer2
    extends Reducer<Text, Text, Text, Text> {
 
        private static Text word_doc_id = new Text();
        private static Text n_N       = new Text();
 
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
             
            String doc_id = key.toString();

            int N = 0;
            HashMap<String, Integer> wordList = new HashMap<String, Integer>();
            wordList.clear();
            for (Text word_n: values) {
                String[] bits = word_n.toString().split(";");
                String word = bits[0];
                int n       = Integer.parseInt(bits[1]);
                wordList.put(word, n);
                N += n;
            }
 
            for (String word: wordList.keySet()) {
                word_doc_id.set(word+"@"+doc_id);
                n_N.set(wordList.get(word)+";"+N);
                context.write(word_doc_id,  n_N);
            }
        }
    }
