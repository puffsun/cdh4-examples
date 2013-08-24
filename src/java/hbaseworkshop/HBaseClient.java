package hbaseworkshop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Following shows how to connect to HBase from java.
 *
 * @author srinath
 */
public class HBaseClient {

    public static void main(String[] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "test");

        //put data
        Put put = new Put("row1".getBytes());
        put.add("cf".getBytes(), "b".getBytes(), "val2".getBytes());
        table.put(put);

        //read data
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("cf"));
        ResultScanner results = table.getScanner(s);

        try {
            for (Result result : results) {
                KeyValue[] keyValuePairs = result.raw();
                System.out.println(new String(result.getRow()));
                for (KeyValue keyValue : keyValuePairs) {
                    System.out.println(new String(keyValue.getFamily()) + " "
                            + new String(keyValue.getQualifier())
                            + "=" + new String(keyValue.getValue()));
                }
            }
        } finally {
            results.close();
        }

    }

}
