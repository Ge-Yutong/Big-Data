import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

public class TopNWordsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (URI cacheFile : cacheFiles) {
                FileSystem fs = FileSystem.get(cacheFile, context.getConfiguration());
                Path path = new Path(cacheFile);
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(TopNWordsDriver.CounterType.INPUT_RECORDS).increment(1);
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            String lowerCaseToken = token.toLowerCase();
            if (!stopWords.contains(lowerCaseToken) && lowerCaseToken.length() > 6) {
                word.set(lowerCaseToken);
                context.write(word, one);
            }
        }
    }
}

