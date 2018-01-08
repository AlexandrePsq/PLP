package pasquiou.taitai.lab2.exercice5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


// * Mapper: --> input  : (byteCount, line)
//			 --> output : (word@doc, 1)


public class Mapper1
    extends Mapper<LongWritable, Text, Text, IntWritable> {
 
        private final static IntWritable one = new IntWritable(1);
        private static       Text   word_doc_id = new Text();
 

        private static Set<String> stopWords;
        static{
            stopWords = new HashSet<String>();
            stopWords.add("about"); stopWords.add("and");
            stopWords.add("are");   stopWords.add("com");
            stopWords.add("for");   stopWords.add("from");
            stopWords.add("how");
            stopWords.add("that");  stopWords.add("the");
            stopWords.add("this");  stopWords.add("was");
            stopWords.add("what");  stopWords.add("when");
            stopWords.add("where"); stopWords.add("with");
            stopWords.add("who");   stopWords.add("will");
            stopWords.add("the");   stopWords.add("www");
             
        }
 
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
 
            String line    = value.toString().toLowerCase();
            String[] words = line.split("\\W+");
 
            // dans un soucis d'efficacite memoire, on enleve parmi les mots de moins de 3 lettres:
            for (int i=0; i < words.length; i++) {
                String word = words[i];
                if (word.length() < 3 					|| // On enleve donc : 
                    stopWords.contains(word)            || // les stopwords
                    Character.isDigit(word.charAt(0))   || // les mots commencant par un chiffre
                    word.contains("_"))   				  // et les mots comntenant '_'
                     {
                    continue;
                }
 
                String doc_id =
                        ((FileSplit) context.getInputSplit()).getPath().getName();
 
                word_doc_id.set(word+"@"+doc_id);
                context.write(word_doc_id, one);
            }
        }
    }