import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileUtil;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TopNWordsDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //set configuration for mapreduce
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "1024");
        conf.set("mapreduce.task.timeout", "600000");
        conf.set("mapreduce.task.io.sort.mb", "256");
        conf.set("mapreduce.map.java.opts", "-Xmx1638m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1638m");
        conf.setInt("mapreduce.job.maps", 4);
        conf.setInt("mapreduce.job.reduces", 2);
        //args
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopNWordsDriver <stopwords> <in> <out>");
            System.exit(2);
        }
        //set temp output path for job1 and it's also the input path for job2
        Path tempOutput = new Path("temp_output");
        FileSystem fs = FileSystem.get(conf);
        //delete temp output path
        if (fs.exists(tempOutput)) {
            fs.delete(tempOutput, true);
        }
        //delete final output path
        Path finalOutput = new Path(args[2]);
        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, true);
        }
        //set configuration of job1(map and count)
        Job job1 = Job.getInstance(conf, "word frequency");
        job1.setJarByClass(TopNWordsDriver.class);
        job1.setMapperClass(TopNWordsMapper.WordFrequencyMapper.class);
        job1.setCombinerClass(TopNWords.TopNWordsCombiner.class);
        job1.setReducerClass(TopNWords.WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.addCacheFile(new Path(otherArgs[0]).toUri());
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job1, tempOutput);
        //set configuration of job2(sort)
        Job job2 = Job.getInstance(conf, "top n words");
        //only one reduce job for global optimum
        job2.setNumReduceTasks(1);
        job2.setJarByClass(TopNWordsDriver.class);
        job2.setMapperClass(TopNWordsMapper.PassThroughMapper.class);
        job2.setReducerClass(TopNWords.TopNWordsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        long startTime = System.currentTimeMillis();
        boolean job1Success = job1.waitForCompletion(true);
        boolean job2Success = false;
        if(job1Success){
            job2Success = job2.waitForCompletion(true);
        }
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        if (job1Success && job2Success) {
            long inputRecords = job1.getCounters().findCounter(CounterType.INPUT_RECORDS).getValue();
            long outputRecords = job2.getCounters().findCounter(CounterType.OUTPUT_RECORDS).getValue();

            // Print the counters to the console
            System.out.println("Input records: " + inputRecords);
            System.out.println("Output records: " + outputRecords);
            System.out.println("Elapsed time: " + elapsedTime + " ms");

            double throughput = (double) inputRecords / (elapsedTime / 1000.0);
            System.out.println("Throughput: " + throughput + " records/sec");

            // Save the counters to a file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("counters.txt"))) {
                writer.write("Input records: " + inputRecords + "\n");
                writer.write("Output records: " + outputRecords + "\n");
                writer.write("Elapsed time: " + elapsedTime + " ms\n");
                writer.write("Throughput: " + throughput + " records/sec\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public enum CounterType {
        INPUT_RECORDS,
        OUTPUT_RECORDS
    }
}