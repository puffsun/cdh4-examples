package orderinversion;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/13/13
 * Time: 11:04 AM
 */
public class PairRelativeOccurrence extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("hadoop PairRelativeOccurrence <input> <output>");
            GenericOptionsParser.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        Job job = new Job(getConf(), "Pair Relative Occurrence");
        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PairRelativeOccurrence(), args);
        System.exit(exitCode);
    }
}
