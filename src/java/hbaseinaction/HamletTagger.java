package hbaseinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 9:49 PM
 */
public class HamletTagger extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "TwitBase Hamlet tagger");
        job.setJarByClass(HamletTagger.class);

        Scan scan = new Scan();
        scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.USER_COL);
        scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);

        TableMapReduceUtil.initTableMapperJob(TwitsDAO.TABLE_NAME, scan, Map.class,
                ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob(Bytes.toString(UsersDAO.TABLE_NAME), IdentityTableReducer.class, job);

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HamletTagger(), args);
        System.exit(exitCode);
    }

    public static class Map extends TableMapper<ImmutableBytesWritable, Put> {

        public static enum Counters {HAMLET_TAGS}

        ;
        private Random rand;

        private boolean mentionsHamlet(String msg) {
            return rand.nextBoolean();
        }

        protected void setup(Context context) {
            rand = new Random(System.currentTimeMillis());
        }

        @Override
        protected void map(ImmutableBytesWritable mapKey, Result result, Context context)
                throws IOException, InterruptedException {

            byte[] byteMsg = result.getColumnLatest(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL).getValue();
            String message = Bytes.toString(byteMsg);

            byte[] bytesUser = result.getColumnLatest(TwitsDAO.TWITS_FAM, TwitsDAO.USER_COL).getValue();
            String user = Bytes.toString(bytesUser);

            if (mentionsHamlet(message)) {
                Put p = UsersDAO.makePut(user, UsersDAO.INFO_FAM, UsersDAO.HAMLET_COL, Bytes.toBytes(true));
                ImmutableBytesWritable outputKey = new ImmutableBytesWritable(p.getRow());
                context.write(outputKey, p);
                context.getCounter(Counters.HAMLET_TAGS).increment(1L);
            }
        }
    }
}
