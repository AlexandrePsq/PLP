package pasquiou.taitai.lab2.exercice5;

 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



// * Mapper: --> input  : (word@doc_id, n;N)
//           --> output : (word, doc_id;n;N;1)
// Le dernier 1 nous permet de compter les documents


public class Mapper3
    extends Mapper<Text, Text, Text, Text> {
 
        private final static Text word      = new Text();
        private static       Text doc_id_n_N_1 = new Text();

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
 
            String[] word_doc_id = key.toString().split("@");
            word.set(word_doc_id[0]);
            String doc_id = word_doc_id[1];
 
            String[] n_N = value.toString().split(";");
            String n = n_N[0];
            String N = n_N[1];
 
            doc_id_n_N_1.set(doc_id +";"+ n +";"+ N +";"+ 1);
            context.write(word, doc_id_n_N_1);
        }
    }