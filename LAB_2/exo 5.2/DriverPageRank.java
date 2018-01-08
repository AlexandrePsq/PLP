package pasquiou.taitai.lab2;
 
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration; 

 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

import pasquiou.taitai.lab2.exercice5.Driver;
import pasquiou.taitai.lab2.exercice5.Mapper2;
import pasquiou.taitai.lab2.exercice5.Reducer2;

public class DriverPageRank extends Configured implements Tool{
    
    public static Integer NodesCount = 0;
    
    public static Set<String> NODES = new HashSet<String>();
    
    public static void main(String[] args) throws Exception {
        
        System.out.println("driver main() args: " + Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new DriverPageRank(), args);
 
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        
        
        System.out.println("driver  run() args: " + Arrays.toString(args));
 
        Configuration conf = new Configuration();
        int step = 0;
 
        // Driver 1     
 
        Job job = Job.getInstance(conf);
        job.setJobName("PageRank" + step);
        System.out.println("job: " + job.getJobName().toString());
        job.setJarByClass(DriverPageRank.class);
 
        // Définition des output pour le mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        // Définition des output pour le reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        //Chemin input/output du maper et du reducer
        conf.set("inputDir", args[0]);
        conf.set("originalInputDir", args[0]);
        conf.set("outputDir", job.getJobName());
 
        FileInputFormat.addInputPath(job, new Path(conf.get("inputDir")));
        
        FileSystem fs1 = FileSystem.get(conf);
        if (fs1.exists(new Path(conf.get("outputDir"))))
            fs1.delete(new Path(conf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job).toString());
 
        job.waitForCompletion(true);
        System.out.println("job completed: " + job.getJobName().toString());
 
        // Driver 2   
 
        conf.set("inputDir", conf.get("outputDir"));
        
        step++;
 
        Job job2 = Job.getInstance(conf);
        job2.setJobName("PageRank" + step);
        System.out.println("job : " + job2.getJobName().toString());
        job2.setJarByClass(DriverPageRank.class);
 
        // Définition des outputs pour le mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
 
        // Définition des outputs pour le reducer
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        conf.set("key.value.separator.in.input.line", "\t");
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
 
        // Format de l'output du reducer
        job2.setOutputFormatClass(TextOutputFormat.class);
 
        //Chemin input/output du maper et du reducer
        conf.set("outputDir", job2.getJobName());
 
        FileInputFormat.addInputPath(job2, new Path(conf.get("inputDir")));        
        
        FileSystem fs2 = FileSystem.get(conf);
        if (fs2.exists(new Path(conf.get("outputDir"))))
            fs2.delete(new Path(conf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job2, new Path(conf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job2))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job2).toString());
 
        job2.waitForCompletion(true);
        System.out.println("job completed: " + job2.getJobName().toString());
 
        return 0;
    }
}