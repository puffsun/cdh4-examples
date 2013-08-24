package hbaseinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 9:09 PM
 */
public class CountShakespeare {


    public static class Map extends TableMapper<Text, LongWritable> {

        public static enum Counters {
            ROWS,
            SHAKESPEAREAN
        }

        private Random rand;

        @Override
        protected void setup(Context context) {
            rand = new Random(System.currentTimeMillis());
        }

        /**
         * Determines if the message pertains to Shakespeare.
         */
        private boolean containsShakespear(String msg) {
            return rand.nextBoolean();
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
                throws IOException, InterruptedException {

            List<KeyValue> column = result.getColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
            if (column != null && column.size() != 0) {
                String message = Bytes.toString(column.get(0).getValue());
                if (message != null && !message.isEmpty()) {
                    context.getCounter(Counters.ROWS).increment(1L);
                    if (containsShakespear(message)) {
                        context.getCounter(Counters.SHAKESPEAREAN).increment(1L);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "TwitBase Shakespeare counter");
        job.setJarByClass(CountShakespeare.class);
        Scan scan = new Scan();
        scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
        TableMapReduceUtil.initTableMapperJob(Bytes.toString(TwitsDAO.TABLE_NAME), scan, Map.class,
                ImmutableBytesWritable.class, Result.class, job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
