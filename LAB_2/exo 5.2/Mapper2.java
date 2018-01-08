package pasquiou.taitai.lab2;


import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

//MapReduce iterative mapper for pageranking
//
//------
// * Mapper: --> input  : (page, page1;page2;...)
//           --> output : (page, pageRank\tsize(allPages))
//               or       (page, @pages)


public class Mapper2
    extends Mapper<Text, Text, Text, Text> {
 
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            

            int IndexSeparateur = value.find("\t");
            
            String Node = key.toString();
            String pageRank = value.toString().split("\t")[0];
            String links = value.toString().split("\t")[1];
            
            String[] allPages = links.split(";");
            

            for (String page : allPages){
                //We write pages linked with their number of links
                Text pageRank_totalLinks = new Text(pageRank + "\t" + allPages.length);
                context.write(new Text(page), pageRank_totalLinks);
            }          
            
            context.write(new Text(Node), new Text("@" + links));
        }
    }