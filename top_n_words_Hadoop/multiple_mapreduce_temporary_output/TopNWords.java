import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TopNWords {

//    public class TopNWordsMapper extends Mapper<Object, Text, Text, IntWritable> {
//
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//        private Set<String> stopWords = new HashSet<>();
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            URI[] cacheFiles = context.getCacheFiles();
//            if (cacheFiles != null && cacheFiles.length > 0) {
//                for (URI cacheFile : cacheFiles) {
//                    FileSystem fs = FileSystem.get(cacheFile, context.getConfiguration());
//                    Path path = new Path(cacheFile);
//                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
//                        String line;
//                        while ((line = bufferedReader.readLine()) != null) {
//                            stopWords.add(line.trim().toLowerCase());
//                        }
//                    }
//                }
//            }
//        }
//
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            context.getCounter(TopNWordsDriver.CounterType.INPUT_RECORDS).increment(1);
//            String[] tokens = value.toString().toLowerCase().split("\\s+");
//            for (String token : tokens) {
//                if (!stopWords.contains(token) && token.length() > 6) {
//                    word.set(token);
//                    context.write(word, one);
//                }
//            }
//        }
//    }

//    public static class TopNWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        private TreeMap<Integer, List<Text>> topNMap = new TreeMap<>();
//        private int n = 100;
//
//        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//
//            List<Text> texts = topNMap.get(sum);
//            if (texts == null) {
//                texts = new ArrayList<>();
//                topNMap.put(sum, texts);
//            }
//            texts.add(new Text(key));
//            context.getCounter(TopNWordsDriver.CounterType.OUTPUT_RECORDS).increment(1);
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            int count = 0;
//            for (Entry<Integer, List<Text>> entry : ((TreeMap<Integer, List<Text>>) topNMap).descendingMap().entrySet()) {
//                for (Text text : entry.getValue()) {
//                    if (count++ >= n) {
//                        break;
//                    }
//                    context.write(text, new IntWritable(entry.getKey()));
//                }
//                if (count >= n) {
//                    break;
//                }
//            }
//        }
//    }

    public static class Pair<K, V extends Comparable<? super V>> implements Comparable<Pair<K, V>> {
        private final K key;
        private final V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public int compareTo(Pair<K, V> o) {
            return this.value.compareTo(o.getValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(key, pair.key) && Objects.equals(value, pair.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }


    public static class TopNWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private class WordCountComparator implements Comparator<Pair<Integer, Text>> {
            @Override
            public int compare(Pair<Integer, Text> a, Pair<Integer, Text> b) {
                return a.getKey().equals(b.getKey()) ? a.getValue().toString().compareTo(b.getValue().toString()) : a.getKey() - b.getKey();
            }
        }

        private PriorityQueue<Pair<Integer, Text>> topNQueue;
        private int n = 100;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topNQueue = new PriorityQueue<>(n, new WordCountComparator());
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topNQueue.add(new Pair<>(sum, new Text(key)));
            if (topNQueue.size() > n) {
                topNQueue.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Pair<Integer, Text>> topNList = new ArrayList<>(topNQueue);
            Collections.sort(topNList, Collections.reverseOrder());

            for (Pair<Integer, Text> entry : topNList) {
                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val:values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopNWordsCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
