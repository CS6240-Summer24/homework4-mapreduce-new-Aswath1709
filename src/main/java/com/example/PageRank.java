package com.example;
import org.apache.log4j.PropertyConfigurator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.List;

public class PageRank {

    // First Iteration Mapper
    public static class FirstIterationMapper extends Mapper<Object, Text, Text, Text> {

        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 1);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().trim().split("\\s+");
            int source = Integer.parseInt(nodes[0]);
            int destination = Integer.parseInt(nodes[1]);

            // Emit node with outlink
            context.write(new Text(Integer.toString(source)), new Text("N:" + destination));

            // Emit outlink with initial PR (1.0 / (k * k))
            context.write(new Text(Integer.toString(destination)), new Text("P:" + (1.0 / (k * k))));
        }
    }

    // First Iteration Reducer
    public static class FirstIterationReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> outlinks = new ArrayList<>();
            double sumOfPRFromInlinks = 0.0;
            for (Text value : values) {
                if (value.toString().startsWith("N:")) {
                    outlinks.add(value.toString().substring(2));
                } else if (value.toString().startsWith("P:")) {
                    sumOfPRFromInlinks += Double.parseDouble(value.toString().substring(2));
                }
            }

            // Calculate new PageRank
            double newPR = 0.15 / (context.getConfiguration().getInt("k", 1) * context.getConfiguration().getInt("k", 1)) +
                    0.85 * sumOfPRFromInlinks;

            // Emit node with outlinks and new PR
            context.write(key, new Text("N:" + outlinks.toString() + " P:" + newPR));
        }
    }

    // Subsequent Iteration Mapper
    public static class IterationMapper extends Mapper<Object, Text, Text, Text> {

        private double dummyPR = 0.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read dummy PR from HDFS file
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path dummyPRPath = new Path(conf.get("previous_iteration_pr_path"), "part-r-00000");

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dummyPRPath)))) {
                String line = reader.readLine();
                if (line != null) {
                    int startIndex = line.indexOf("P:") + 2;
                    String prValueString = line.substring(startIndex);
                    dummyPR = Double.parseDouble(prValueString) / (conf.getInt("k", 1) * conf.getInt("k", 1));
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\\s+", 2);
            String node = parts[0];
            if(!node.equals("0")){
            String[] data = parts[1].split(" ");
            String outlink = data[0].substring(3, 4);

            // Emit node with outlink
            context.write(new Text(node), new Text("N:" + outlink));

            // Emit outlink with PR contribution
            double pr = Double.parseDouble(data[1].substring(2)) + dummyPR;
            context.write(new Text(outlink), new Text("P:" + pr));
        }}
    }

    // Subsequent Iteration Reducer
    public static class IterationReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> outlinks = new ArrayList<>();
            double sumOfPRFromInlinks = 0.0;

            for (Text value : values) {
                if (value.toString().startsWith("N:")) { 
                    outlinks.add(value.toString().substring(2));
                } else if (value.toString().startsWith("P:")) {
                    sumOfPRFromInlinks += Double.parseDouble(value.toString().substring(2));
                }
            }

            // Calculate new PageRank
            double newPR = 0.15 / (context.getConfiguration().getInt("k", 1) * context.getConfiguration().getInt("k", 1)) +
                    0.85 * sumOfPRFromInlinks;

            // Emit node with outlinks and new PR
            context.write(key, new Text("N:" + outlinks.toString() + " P:" + newPR));
        }
    }

    // Final Mapper to Emit (node, PR) pairs from the last iteration
    public static class FinalMapper extends Mapper<Object, Text, Text, Text> {

        private double dummyPR = 0.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read dummy PR from HDFS file
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path dummyPRPath = new Path(conf.get("previous_iteration_pr_path"), "part-r-00000");

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dummyPRPath)))) {
                String line = reader.readLine();
                if (line != null) {
                    int startIndex = line.indexOf("P:") + 2;
                    String prValueString = line.substring(startIndex);
                    dummyPR = Double.parseDouble(prValueString) / (conf.getInt("k", 1) * conf.getInt("k", 1));
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\\s+", 2);
            String node = parts[0];
            if(!node.equals("0")){
            String[] data = parts[1].split(" ");  
            // Emit outlink with PR contribution
            double pr = Double.parseDouble(data[1].substring(2)) + dummyPR;
            context.write(new Text(node), new Text(Double.toString(pr)));
        }}
    
    }

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
        if (args.length < 3) {
            System.err.println("Usage: PageRank <input-path> <output-path> <iterations>");
            System.exit(1);
        }

        int iterations = Integer.parseInt(args[2]);
        int k = (int) Math.sqrt(getNumberOfNodes(new Path(args[0])));

        Configuration conf = new Configuration();
        conf.setInt("k", k);

        // Perform the first iteration
        Job firstJob = Job.getInstance(conf, "First PageRank Iteration");
        firstJob.setJarByClass(PageRank.class);
        firstJob.setMapperClass(FirstIterationMapper.class);
        firstJob.setReducerClass(FirstIterationReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        Path firstOutputPath = new Path(args[1], "iter1");
        FileOutputFormat.setOutputPath(firstJob, firstOutputPath);
        firstJob.waitForCompletion(true);

        // Perform subsequent iterations
        Path previousIterationOutputPath = firstOutputPath;
        for (int i = 1; i <iterations; i++) {
            conf.set("previous_iteration_pr_path", previousIterationOutputPath.toString());
            Job iterationJob = Job.getInstance(conf, "PageRank Iteration " + Integer.toString(i+1));
            iterationJob.setJarByClass(PageRank.class);
            iterationJob.setMapperClass(IterationMapper.class);
            iterationJob.setReducerClass(IterationReducer.class);
            iterationJob.setOutputKeyClass(Text.class);
            iterationJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(iterationJob, previousIterationOutputPath);
            Path outputPath = new Path(args[1], "iter" + i+1);
            FileOutputFormat.setOutputPath(iterationJob, outputPath);
            iterationJob.waitForCompletion(true);
            previousIterationOutputPath = outputPath; // Set the output path of current iteration as input for next iteration
        }

        // Perform final iteration to emit (node, PR) pairs
        conf.set("previous_iteration_pr_path", previousIterationOutputPath.toString());
        Job finalJob = Job.getInstance(conf, "Final PageRank Iteration");
        finalJob.setJarByClass(PageRank.class);
        finalJob.setMapperClass(FinalMapper.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(finalJob, previousIterationOutputPath);
        Path finalOutputPath = new Path(args[1], "final_output");
        FileOutputFormat.setOutputPath(finalJob, finalOutputPath);
        finalJob.waitForCompletion(true);

        System.exit(0);
    }

    // Helper method to get number of nodes (k * k)
    private static long getNumberOfNodes(Path inputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
            long count = br.lines().count();
            return count;
        }
    }
}
