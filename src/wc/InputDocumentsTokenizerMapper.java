package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Hold partial input words as well as its count in memory, until all input key-value pairs of the input
 * data split has been processed.
 * <p/>
 * User: George Sun
 * Date: 7/10/13
 * Time: 8:17 PM
 */
public class InputDocumentsTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Map<String, Integer> wordCounter;
    private final Text wordText = new Text();
    private final IntWritable totalCountInDocument = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        wordCounter = new HashMap<String, Integer>();
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

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
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Emit each word as well as its count
        for (Map.Entry<String, Integer> entry : wordCounter.entrySet()) {
            wordText.set(entry.getKey());
            totalCountInDocument.set(entry.getValue());
            context.write(wordText, totalCountInDocument);
        }
        super.cleanup(context);
    }
}
