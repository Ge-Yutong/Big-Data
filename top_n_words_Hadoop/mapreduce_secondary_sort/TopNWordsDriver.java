import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
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
        conf.setLong("mapreduce.input.fileinputformat.split.minsize", 128 * 1024 * 1024); // 10 MB
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024); // 25 MB
        conf.setInt("top.n.words", 100);
        conf.setInt("mapreduce.map.cpu.vcores", 4);
        conf.setInt("mapreduce.reduce.cpu.vcores", 4);
        conf.setInt("mapreduce.job.maps", 4);
        conf.setInt("mapreduce.job.reduces", 1);
        conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.95f);

        //args
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopNWordsDriver <stopwords> <in> <out>");
            System.exit(2);
        }
        FileSystem fs = FileSystem.get(conf);
        //delete final output path
        Path finalOutput = new Path(args[2]);
        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, true);
        }
        Job job = Job.getInstance(conf, "top n words");
        job.setJarByClass(TopNWordsDriver.class);
        job.setMapperClass(TopNWordsMapper.class);
        job.setCombinerClass(TopNWords.TopNWordsCombiner.class);
        job.setReducerClass(TopNWords.TopNWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(otherArgs[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        long startTime = System.currentTimeMillis();
        boolean jobSuccess = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        if (jobSuccess) {
            long inputRecords = job.getCounters().findCounter(CounterType.INPUT_RECORDS).getValue();
            long outputRecords = job.getCounters().findCounter(CounterType.OUTPUT_RECORDS).getValue();

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