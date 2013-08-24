package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Keep reading until the whole document stream closed, then emit each term as well as its total count.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/10/13
 * Time: 7:56 PM
 */
public class DocumentTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final Text wordText = new Text();
    private final IntWritable totalCountInDocument = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> wordCounter = new HashMap<String, Integer>();
        StringTokenizer st = new StringTokenizer(value.toString());
        // Count every word in a document
        while (st.hasMoreTokens()) {
            String word = st.nextToken();
            if (wordCounter.containsKey(word)) {
                wordCounter.put(word, wordCounter.get(word) + 1);
            } else {
                wordCounter.put(word, 1);
            }
        }

        // Emit each word as well as its count
        for (Map.Entry<String, Integer> entry : wordCounter.entrySet()) {
            wordText.set(entry.getKey());
            totalCountInDocument.set(entry.getValue());
            context.write(wordText, totalCountInDocument);
        }
    }
}
