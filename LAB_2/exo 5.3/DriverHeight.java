package pasquiou.taitai.lab2.exercice53;
 
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration; 

 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

public class DriverHeight extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
 
        System.out.println("driver main() args: " + Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new DriverHeight(), args);
 
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        System.out.println("driver  run() args: " + Arrays.toString(args));
 
        Configuration conf = new Configuration();
 
        // Driver 1 
        // input  (byteCount, line)
        // output (espece, especeCount)       
 
        Job job1 = Job.getInstance(conf);
        job1.setJobName("MaxHeight");
        System.out.println("job: " + job1.getJobName().toString());
        job1.setJarByClass(DriverArbre.class);
 
        // Définition des output pour le mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
 
        // Définition des output pour le reducer
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
 

        job1.setMapperClass(MapperHeight.class);
        job1.setReducerClass(ReducerHeight.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
 
        //Chemin input/output du maper et du reducer
        conf.set("inputDir", args[0]);
        conf.set("originalInputDir", args[0]);
        conf.set("outputDir", job1.getJobName());
 
        FileInputFormat.addInputPath(job1, new Path(conf.get("inputDir")));
        
        FileSystem fs1 = FileSystem.get(conf);
        if (fs1.exists(new Path(conf.get("outputDir"))))
            fs1.delete(new Path(conf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job1, new Path(conf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job1))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job1).toString());
 
        job1.waitForCompletion(true);
        System.out.println("job completed: " + job1.getJobName().toString());
        
        return 0;
    }
}