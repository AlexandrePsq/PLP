package pasquiou.taitai.lab2.exercice5;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// * Reducer: --> input  : (word, [doc_id;n;N;count, doc_id;n;N;count, ...])
//            --> output : (word@doc_id, tf_idf)


public class Reducer3
    extends Reducer<Text, Text, Text, DoubleWritable> {
 
        private static Text word_doc_id = new Text();
        private static DoubleWritable tf_idf   = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
 
            HashMap<String, Integer> word_doc_idList = new HashMap<String, Integer>();
            HashMap<String, Integer> doc_idList      = new HashMap<String, Integer>();
            HashMap<String, Integer> wordList     = new HashMap<String, Integer>();
            word_doc_idList.clear();
            doc_idList.clear();
            wordList.clear();
 
            String word = key.toString();
 
            for (Text value: values) {
                String[] doc_id_n_N_count = value.toString().split(";");
                String  doc_id    = doc_id_n_N_count[0];
                Integer n      = Integer.parseInt(doc_id_n_N_count[1]);
                Integer N      = Integer.parseInt(doc_id_n_N_count[2]);
                Integer count  = Integer.parseInt(doc_id_n_N_count[3]);
 
                // save each doc_id and the total number of words in it
                if (!doc_idList.containsKey(doc_id)) {
                    doc_idList.put(doc_id,N);
                } else {
                    if (N != doc_idList.get(doc_id)) {
                        System.out.println("N != doc_idList.get(doc_id)"+
                                ": N="+N+
                                "; doc_id="+doc_id+
                                "; doc_idList.get(doc_id)="+doc_idList.get(doc_id));
                        System.exit(-1);
                    }
                }
                 
                // save the key 'word@doc_id' and the frequency of word in doc_id 'n'
                if (!word_doc_idList.containsKey(word+"@"+doc_id)) {
                    word_doc_idList.put(word+"@"+doc_id,n);
                } else {
                    if (n != word_doc_idList.get(word+"@"+doc_id)) {
                        System.out.println("n != word_doc_idList.get(word+\"@\"+doc_id)"+
                                ": n="+n+
                                "; (word+\"@\"+doc_id="+word+"@"+doc_id+
                                "; word_doc_idList="+word_doc_idList.get(word+"@"+doc_id));
                        System.exit(-1);
                    }
                }
                 
                // increment the number of doc in which 'word' appears
                if (!wordList.containsKey(word)) {
                    wordList.put(word, count);
                } else {
                    Integer df = wordList.get(word);
                    df += count;
                    wordList.put(word, df);
                }
            }

            // compose the (word@doc_id, n;N;df) output
            for (String Worddoc_id: word_doc_idList.keySet()) {
                String[] Word_doc_id = Worddoc_id.split("@");
                String Word       			= Word_doc_id[0];
                String doc_id       		= Word_doc_id[1];
                double occurences 			= word_doc_idList.get(Worddoc_id); //occurence in doc_id
                double nb_words_doc    	= doc_idList.get(doc_id); // number of words in doc_id
                Integer nb_doc_with_word    = wordList.get(Word); //number of docs where word appears
                Integer nb_doc 				= 2; //number of doc
 
                word_doc_id.set(Worddoc_id); // word@doc_id
                tf_idf.set((occurences/nb_words_doc)*(Math.log10(new Double(nb_doc/nb_doc_with_word))));
                context.write(word_doc_id,  tf_idf);
            }
        }
    }