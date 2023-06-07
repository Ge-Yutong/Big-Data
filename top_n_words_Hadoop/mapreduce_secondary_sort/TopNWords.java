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


    public static class TopNWordsReducer extends Reducer<WordFrequencyWritable, IntWritable, Text, IntWritable> {

        private class WordCountComparator implements Comparator<Pair<Integer, Text>> {
            @Override
            public int compare(Pair<Integer, Text> a, Pair<Integer, Text> b) {
                return a.getKey().equals(b.getKey()) ? a.getValue().toString().compareTo(b.getValue().toString()) : a.getKey() - b.getKey();
            }
        }

        private PriorityQueue<Pair<Integer, Text>> topNQueue;
        private int n;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            n = context.getConfiguration().getInt("top.n.words", 100);
            topNQueue = new PriorityQueue<>(n, new WordCountComparator());
        }

        public void reduce(WordFrequencyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topNQueue.add(new Pair<>(sum, key.getWord()));
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



    public static class TopNWordsCombiner extends Reducer<WordFrequencyWritable, IntWritable, WordFrequencyWritable, IntWritable> {
        public void reduce(WordFrequencyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
