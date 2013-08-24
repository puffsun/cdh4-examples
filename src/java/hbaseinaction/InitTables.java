package hbaseinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 5:44 PM
 */
public class InitTables {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (args.length > 0 && "-f".equalsIgnoreCase(args[0])) {
            // prompt user for the deletion.
            System.out.println("!!! dropping tables in...");
            for (int i = 5; i > 0; i--) {
                System.out.println(i);
                Thread.sleep(1000);
            }

            deleteHBaseTable(admin, UsersDAO.TABLE_NAME);
            deleteHBaseTable(admin, TwitsDAO.TABLE_NAME);
        }

        createHBaseTable(admin, UsersDAO.TABLE_NAME, UsersDAO.INFO_FAM);
        createHBaseTable(admin, TwitsDAO.TABLE_NAME, TwitsDAO.TWITS_FAM);
    }

    private static void deleteHBaseTable(HBaseAdmin admin, byte[] tableName) throws IOException {
        if (admin == null || tableName == null) {
            throw new RuntimeException("Null parameters.");
        }

        if (admin.tableExists(tableName)) {
            System.out.printf("Deleting table %s\n", Bytes.toString(tableName));
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
    }

    private static void createHBaseTable(HBaseAdmin admin, byte[] tableName,
                                         byte[] columnFamily) throws IOException {
        if (admin == null || tableName == null) {
            throw new RuntimeException("Null parameters");
        }

        if (admin.tableExists(tableName)) {
            System.out.printf("%s table already exists.\n", Bytes.toString(tableName));
        } else {
            System.out.printf("Creating %s table...\n", Bytes.toString(tableName));
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
            System.out.printf("%s table created.\n", Bytes.toString(tableName));
        }
    }
}
