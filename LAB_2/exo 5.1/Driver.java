package pasquiou.taitai.lab2.exercice5;
 
import java.util.Arrays;
import org.apache.hadoop.fs.FileSystem;  

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration; 

 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
 
        System.out.println("driver main() args: " + Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
 
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        System.out.println("driver  run() args: " + Arrays.toString(args));
 
        Configuration conf = new Configuration();
        int step = 1;
 
        // Driver 1 
        // input  (byteCount, line)
        // output (word@doc, n)       n = frequence des mots dans un document
 
        Job job1 = Job.getInstance(conf);
        job1.setJobName("TFIDF" + step);
        System.out.println("job: " + job1.getJobName().toString());
        job1.setJarByClass(Driver.class);
 
        // Définition des output pour le mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
 
        // Définition des output pour le reducer
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
 

        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
 
        //Chemin input/output du maper et du reducer
        conf.set("inputDir1", args[0]);
        conf.set("inputDir2", args[1]);
        conf.set("originalInputDir", args[0]);
        conf.set("outputDir", job1.getJobName());
 
        FileInputFormat.addInputPath(job1, new Path(conf.get("inputDir1")));
        FileInputFormat.addInputPath(job1, new Path(conf.get("inputDir2")));
        
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
 
        // Driver 2
        // input  (word@doc, n)         n = frequence des mots dans un document
        // output (word@doc, n;N)       N = nombre total de mots dans un document
 
        conf.set("inputDir", conf.get("outputDir"));
        
        step++;
 
        Job job2 = Job.getInstance(conf);
        job2.setJobName("TFIDF" + step);
        System.out.println("job : " + job2.getJobName().toString());
        job2.setJarByClass(Driver.class);
 
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
 
        // Driver 3
        // input  (word@doc, n;N)       n  = frequence des mots dans un document
        //                              N  = nombre total de mots dans un document
        // output (word@doc, n;N;df)    df = frequence de mots
 
        conf.set("inputDir", conf.get("outputDir"));
        
        step++;
 
        Job job3 = Job.getInstance(conf);
        job3.setJobName("TFIDF" + step);
        System.out.println("job : " + job3.getJobName().toString());
        job3.setJarByClass(Driver.class);
 
        // Definition des output du mapper
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
 
        // Definition des outputs du reducer
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
 
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        conf.set("key.value.separator.in.input.line", "\t");
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
 
        // Format de l'output du reducer
        job3.setOutputFormatClass(TextOutputFormat.class);
 
        // Chemin input/output du maper et du reducer
        conf.set("outputDir", job3.getJobName());
 
        FileInputFormat.addInputPath(job3, new Path(conf.get("inputDir")));
        
        FileSystem fs3 = FileSystem.get(conf);
        if (fs3.exists(new Path(conf.get("outputDir"))))
            fs3.delete(new Path(conf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job3, new Path(conf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job3))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job3).toString());
 
        job3.waitForCompletion(true);
        System.out.println("job completed: " + job3.getJobName().toString());
 
        
        return 0;
    }
}