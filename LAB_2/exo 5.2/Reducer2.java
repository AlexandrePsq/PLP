package pasquiou.taitai.lab2;


import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;


//
//------
// * Reducer: --> input  : ((page, [pageRank\tsize(allPages)), ...])
//                or       ((page, [@pages), ...])
//            --> output : (page, pageRank)


public class Reducer2
    extends Reducer<Text, Text, Text, Text> {
        
    public static Double DAMPING = 0.85;
    
 
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String links = "";
            double sumPageRankOtherLinks = 0.0;
            
            for (Text value: values) {
                String content = value.toString();
                if (content.startsWith("@")) {
                    //In this case we have all the links which starts with @
                    System.out.println("yes");
                    content = content.substring(1);
                    links += content;
                }
                else {
                    // In this case, its the number of links of the adjacent node
                    System.out.println("no");
                    String[] info = content.split("\\t");
                    System.out.println(info.length);
                    //int otherLinks = Integer.parseInt(info[1]);
                    //double pageRank = Double.parseDouble(info[0]);
                    
                    //sumPageRankOtherLinks +=  (pageRank / otherLinks);    
                }   
            }
            
            double newRank = DAMPING * sumPageRankOtherLinks + (1 - DAMPING);
            
            context.write(key, new Text(String.valueOf(newRank)));
        }
    }