package hbaseinaction;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class demonstrates the use of the MultiTableOutputFormat class.
 * Using this class we can write the output of a Hadoop map reduce program
 * into different HBase table.
 *
 * @author Wildnove
 * @version 1.0 19 Jul 2011
 */
public class MultiTableOutputTutorial extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MultiTableOutputTutorial.class);
    private static final String CMDLINE = "com.wildnove.tutorial.MultiTableOutputTutorial " +
            "<inputFile> [-n name] [-s]";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MultiTableOutputTutorial(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        HelpFormatter help = new HelpFormatter();
        Options options = new Options();
        options.addOption("h", "help", false, "print program usage");
        options.addOption("n", "name", true, "sets job name");
        CommandLineParser parser = new BasicParser();
        CommandLine cline;
        try {
            cline = parser.parse(options, args);
            args = cline.getArgs();
            if (args.length < 1) {
                help.printHelp(CMDLINE, options);
                return -1;
            }
        } catch (ParseException e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }

        String name = null;
        try {
            if (cline.hasOption('n'))
                name = cline.getOptionValue('n');
            else
                name = "wildnove.com - Tutorial MultiTableOutputFormat ";
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(conf);
            Path inputFile = new Path(fs.makeQualified(new Path(args[0])).toUri().getPath());
            if (!getMultiTableOutputJob(name, inputFile).waitForCompletion(true))
                return -1;
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            help.printHelp(CMDLINE, options);
            return -1;
        }
        return 0;
    }

    /**
     * Here we configure our job to use MultiTableOutputFormat class as map reduce output.
     * Note that we use 1 reduce only for debugging purpose, but you can use more than 1 reduce.
     */
    private Job getMultiTableOutputJob(String name, Path inputFile) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info(name + " starting...");
            LOG.info("computing file: " + inputFile);
        }
        Job job = new Job(getConf(), name);
        job.setJarByClass(MultiTableOutputTutorial.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputFile);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(MyReducer.class);

        return job;
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        /**
         * The map method splits the csv file according to this structure
         * brand,model,size (e.g. Cadillac,Seville,Midsize) and output all data using
         * brand as key and the couple model,size as value.
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valueSplitted = value.toString().split(",");
            if (valueSplitted.length == 3) {
                String brand = valueSplitted[0];
                String model = valueSplitted[1];
                String size = valueSplitted[2];

                outKey.set(brand);
                outValue.set(model + "," + size);
                context.write(outKey, outValue);
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, ImmutableBytesWritable, Writable> {

        /**
         * The reduce method fill the TestCars table with all csv data,
         * compute some counters and save those counters into the TestBrandsSizes table.
         * So we use two different HBase table as output for the reduce method.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> statsSizeCounters = new HashMap<String, Integer>();
            String brand = key.toString();
            // We are receiving all models,size grouped by brand.
            for (Text value : values) {
                String[] valueSplitted = value.toString().split(",");
                if (valueSplitted.length == 2) {
                    String model = valueSplitted[0];
                    String size = valueSplitted[1];

                    // Fill the TestCars table
                    ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.toBytes("TestCars"));
                    byte[] putKey = Bytes.toBytes(brand + "," + model);
                    byte[] putFamily = Bytes.toBytes("Car");
                    Put put = new Put(putKey);
                    // qualifier brand
                    byte[] putQualifier = Bytes.toBytes("brand");
                    byte[] putValue = Bytes.toBytes(brand);
                    put.add(putFamily, putQualifier, putValue);
                    // qualifier model
                    putQualifier = Bytes.toBytes("model");
                    putValue = Bytes.toBytes(model);
                    put.add(putFamily, putQualifier, putValue);
                    // qualifier size
                    putQualifier = Bytes.toBytes("size");
                    putValue = Bytes.toBytes(size);
                    put.add(putFamily, putQualifier, putValue);
                    context.write(putTable, put);

                    // Compute some counters: number of different sizes for a brand
                    if (!statsSizeCounters.containsKey(size))
                        statsSizeCounters.put(size, 1);
                    else
                        statsSizeCounters.put(size, statsSizeCounters.get(size) + 1);
                }
            }

            for (Entry<String, Integer> entry : statsSizeCounters.entrySet()) {
                // Fill the TestBrandsSizes table
                ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.toBytes("TestBrandsSizes"));
                byte[] putKey = Bytes.toBytes(brand);
                byte[] putFamily = Bytes.toBytes("BrandSizes");
                Put put = new Put(putKey);
                // We can use as qualifier the sizes
                byte[] putQualifier = Bytes.toBytes(entry.getKey());
                byte[] putValue = Bytes.toBytes(entry.getValue());
                put.add(putFamily, putQualifier, putValue);
                context.write(putTable, put);
            }
        }
    }
}
