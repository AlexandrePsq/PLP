package pasquiou.taitai.lab2.exercice5;


import org.apache.hadoop.io.Text;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;




// * Mapper: --> input  : (word@doc_id, n)
//			 --> output : (doc_id, word;n)


public class Mapper2
extends Mapper<Text, Text, Text, Text> {

    private final static Text doc_id    = new Text();
    private static       Text word_n = new Text();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] word_doc_id = key.toString().split("@");
        String word       = word_doc_id[0];
        doc_id.set(word_doc_id[1]);
         
        String n = value.toString();
        word_n.set(word+";"+n);

        context.write(doc_id, word_n);
    }
}