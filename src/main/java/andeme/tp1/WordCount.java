package andeme.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class WordCount {
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        /* Creates a new Job with no particular Cluster and a given jobName.
           A Cluster will be created from the conf parameter only when it's needed.
           The Job makes a copy of the Configuration so that any necessary internal modifications
           do not reflect on the incoming parameter.
        */
        Job job = Job.getInstance(conf, "word count");

        // Set the Jar by finding where a given class came from.
        job.setJarByClass(WordCount.class);

        // Set the Mapper for the job.
        job.setMapperClass(TokenizerMapper.class);

        // Set the combiner class for the job.
        job.setCombinerClass(IntSumReducer.class);

        // Set the Reducer for the job.
        job.setReducerClass(IntSumReducer.class);

        // Set the key class for the job output data.
        job.setOutputKeyClass(Text.class);

        // Set the value class for job outputs.
        job.setOutputValueClass(IntWritable.class);

        // Add a Path to the list of inputs for the map-reduce job.
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the Path of the output directory for the map-reduce job.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job to the cluster and wait for it to finish.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
