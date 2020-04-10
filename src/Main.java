import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static java.lang.System.err;
import static java.lang.System.exit;

public class Main extends Configured {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] jobArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (jobArgs.length == 0) {
            err.println("Error: Please add output argument");
            exit(1);
        } else if (jobArgs.length != 1) {
            err.println("Error: Jar accepts only one argument");
            exit(1);
        }

        System.out.println("Starting...");
        try {
            //First job returns mean score for every film genre
            Job meanScoreJob = Job.getInstance(conf, "IMDb_movies_MeanScoreMapReduce");

            meanScoreJob.setJarByClass(MeanScoreMapReduce.class);
            meanScoreJob.setMapperClass(MeanScoreMapReduce.MeanMapper.class);
            meanScoreJob.setCombinerClass(MeanScoreMapReduce.MeanReducer.class);
            meanScoreJob.setPartitionerClass(MeanScoreMapReduce.MeanPartitioner.class);
            meanScoreJob.setReducerClass(MeanScoreMapReduce.MeanReducer.class);
            meanScoreJob.setOutputKeyClass(Text.class);
            meanScoreJob.setOutputValueClass(FloatWritable.class);

            Path meanScoreJobInput = new Path("/user/maliszewjaku/IMDb_movies.csv");
            Path meanScoreJobOutput = new Path(jobArgs[0], "meanScoreJobOutput");
            FileInputFormat.addInputPath(meanScoreJob, meanScoreJobInput);
            FileOutputFormat.setOutputPath(meanScoreJob, meanScoreJobOutput);

            //There are 26 film genres in data file
            meanScoreJob.setNumReduceTasks(26);

            if (!meanScoreJob.waitForCompletion(true)) {
                System.out.println("First job failed");
                System.exit(1);
            }

            System.out.println("...first job finished...");

            //Second job returns films above mean for specific genre, sorted by genre descending
            Job moviesScoreJob = Job.getInstance(conf, "IMDb_movies_MoviesScoreMapReduce");
            moviesScoreJob.setJarByClass(MoviesScoreMapReduce.class);

            //Second job got 2 mappers and 2 input files, MoviesMapper2 handles original data file
            MultipleInputs.addInputPath(moviesScoreJob, meanScoreJobInput, TextInputFormat.class, MoviesScoreMapReduce.MoviesMapper2.class);
            //Mapper 1 handles output from first job
            MultipleInputs.addInputPath(moviesScoreJob, meanScoreJobOutput, TextInputFormat.class, MoviesScoreMapReduce.MoviesMapper1.class);
            moviesScoreJob.setPartitionerClass(MoviesScoreMapReduce.MeanPartitioner.class);
            moviesScoreJob.setReducerClass(MoviesScoreMapReduce.MoviesReducer.class);
            moviesScoreJob.setOutputKeyClass(Text.class);
            moviesScoreJob.setOutputValueClass(Text.class);

            Path moviesScoreJobOutput = new Path(jobArgs[0], "moviesJobOutput");
            FileOutputFormat.setOutputPath(moviesScoreJob, moviesScoreJobOutput);

            moviesScoreJob.setNumReduceTasks(26);
            if (!moviesScoreJob.waitForCompletion(true)) {
                System.out.println("Second job failed");
                System.exit(1);
            } else {
                System.out.println("...both jobs finished!");
                System.exit(0);
            }
            //Every class exits on error to ensure successful execution
        } catch (Exception e) {
            e.fillInStackTrace();
            System.err.println(e.getMessage());
            e.printStackTrace();
            exit(1);
        }
    }
}
