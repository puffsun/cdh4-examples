package orderinversion;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/13/13
 * Time: 10:45 AM
 */
public class WordPair implements WritableComparable<WordPair> {

    private Text word;
    private Text neighbor;

    public WordPair() {
        set(new Text(), new Text());
    }

    public WordPair(Text word, Text neighbor) {
        set(word, neighbor);
    }

    protected void set(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public Text getWord() {
        return word;
    }

    public Text getNeighbor() {
        return neighbor;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public void setNeighbor(Text neighbor) {
        this.neighbor = neighbor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WordPair)) return false;

        WordPair wordPair = (WordPair) o;

        return word.equals(wordPair.getWord()) && neighbor.equals(wordPair.getNeighbor());
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 31 * result + (neighbor != null ? neighbor.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(WordPair other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.getNeighbor());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbor.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbor.readFields(in);
    }
}
