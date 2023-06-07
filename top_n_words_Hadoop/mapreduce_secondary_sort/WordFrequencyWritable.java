import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordFrequencyWritable implements WritableComparable<WordFrequencyWritable> {
    private Text word;
    private IntWritable frequency;

    public WordFrequencyWritable() {
        set(new Text(), new IntWritable());
    }

    public WordFrequencyWritable(Text word, IntWritable frequency) {
        set(word, frequency);
    }


    public void set(Text word, IntWritable frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getFrequency() {
        return frequency;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        frequency.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        frequency.readFields(in);
    }

    @Override
    public int compareTo(WordFrequencyWritable other) {
        int cmp = word.compareTo(other.word);
        if (cmp != 0) {
            return cmp;
        }
        return -frequency.compareTo(other.frequency);
    }
}
