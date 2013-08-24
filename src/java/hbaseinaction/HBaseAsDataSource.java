package hbaseinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HBase as a data source example, you can write your own code in map(...) to read
 * data from HBase from the table specified during job initialization.
 * In this case, the table is your_hbase_table_name.
 * <p/>
 * User: George Sun
 * Date: 7/21/13
 * Time: 12:42 AM
 */
public class HBaseAsDataSource extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "ExampleRead");
        job.setJarByClass(HBaseAsDataSource.class);     // class that contains mapper

        Scan scan = new Scan();
        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCaching(500);
        // don't set to true for MR jobs
        scan.setCacheBlocks(false);
        // set other scan attrs here...

        TableMapReduceUtil.initTableMapperJob(
                // input HBase table name
                "your_hbase_table_name",
                // Scan instance to control column family and attribute selection
                scan,
                MyMapper.class,   // mapper
                null,             // mapper output key
                null,             // mapper output value
                job);
        // because we aren't emitting anything from mapper
        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseAsDataSource(), args);
        System.exit(exitCode);
    }


    public static class MyMapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result result, Context context)
                throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            // For example, read data from HBase table, then populate it into HDFS.
        }
    }
}
