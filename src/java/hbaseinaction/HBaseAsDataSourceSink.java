package hbaseinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HBase is used as data source as well as data sink. This MapReduce job will try to copy data from
 * the source table to the target table. Note that no reduce task needed.
 * <p/>
 * User: George Sun
 * Date: 7/21/13
 * Time: 12:55 AM
 */
public class HBaseAsDataSourceSink extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "ExampleReadWrite");
        job.setJarByClass(HBaseAsDataSourceSink.class);    // class that contains mapper

        Scan scan = new Scan();
        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCaching(500);
        // don't set to true for MR jobs
        scan.setCacheBlocks(false);
        // set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                // input table
                "your_hbase_source_table",
                // Scan instance to control CF and attribute selection
                scan,
                // mapper class
                MyMapper.class,
                // mapper output key
                null,
                // mapper output value
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                // output table
                "your_hbase_target_table",
                // reducer class
                null,
                job);
        // No reducer actually needed,
        // TableOutputFormat will take care of sending the Put to target table.
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseAsDataSourceSink(), args);
        System.exit(exitCode);
    }

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {

            // this example is just copying the data from the source table...
            context.write(row, resultToPut(row, value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (KeyValue kv : result.raw()) {
                put.add(kv);
            }
            return put;
        }
    }
}
