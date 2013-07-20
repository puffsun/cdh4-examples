package wc;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount02 extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //conf.set("mapred.job.tracker", "192.168.1.201:9001");

        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount02.class);
        job.setMapperClass(TokenizerMapper.class);

//	    job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        System.out.println("Task name: " + job.getJobName());
        System.out.println("Task success? " + (job.isSuccessful() ? "Yes" : "No"));

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();

        int res = ToolRunner.run(new Configuration(), new WordCount02(), args);

        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 60000.0);
        System.out.println("Task start: " + formatter.format(start));
        System.out.println("Task end: " + formatter.format(end));
        System.out.println("Time elapsed: " + String.valueOf(time) + " minutes.");

        System.exit(res);
    }

}
