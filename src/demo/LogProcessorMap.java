package demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogProcessorMap extends Mapper<Object, LogWritable, Text, IntWritable> {

    public static enum LOG_PROCESSOR_COUNTER {
        BAD_RECORDS,
        PROCCESSED_RECORDS
    }

    ;

    public void map(Object key, LogWritable value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1);

        // make bytes longWritable and output two value types...
        context.write(value.getUserIP(), value.getResponseSize());
    }
}
