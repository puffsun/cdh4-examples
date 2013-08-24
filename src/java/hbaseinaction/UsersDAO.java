package hbaseinaction;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/20/13
 * Time: 12:52 PM
 */
public class UsersDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("users");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");

    private static final byte[] USER_COL = Bytes.toBytes("user");
    private static final byte[] NAME_COL = Bytes.toBytes("name");
    private static final byte[] EMAIL_COL = Bytes.toBytes("email");
    private static final byte[] PASS_COL = Bytes.toBytes("password");
    public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");
    public static final byte[] HAMLET_COL  = Bytes.toBytes("hamlet_tag");

    private HTablePool pool;

    public UsersDAO(HTablePool pool) {
        this.pool = pool;
    }

    private static Get makeGet(String user) {
        Get g = new Get(Bytes.toBytes(user));
        g.addFamily(INFO_FAM);
        return g;
    }

    private Put makePut(User u) {
        Put p = new Put(Bytes.toBytes(u.user));
        p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
        p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
        p.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
        p.add(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
        p.add(INFO_FAM, TWEETS_COL, Bytes.toBytes(u.tweetCount));
        return p;
    }

    private Delete makeDelete(String user) {
        return new Delete(Bytes.toBytes(user));
    }

    private static Scan makeScan() {
        Scan s = new Scan();
        s.addFamily(INFO_FAM);
        return s;
    }

    public void addUser(String user, String name, String email, String password) throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);
        Put p = makePut(new User(user, name, email, password));
        users.put(p);
        users.close();
    }

    public AbstractUser getUser(String user) throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);
        Get g = makeGet(user);
        Result result = users.get(g);
        if (result.isEmpty()) {
            return null;
        }

        User u = new User(result);
        users.close();
        return u;
    }

    public void deleteUser(String user) throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);
        Delete d = makeDelete(user);
        users.delete(d);
        users.close();
    }

    public List<AbstractUser> getUsers() throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);
        ResultScanner results = users.getScanner(makeScan());
        List<AbstractUser> ret = new ArrayList<AbstractUser>();
        for (Result r : results) {
            ret.add(new User(r));
        }

        users.close();
        return ret;
    }

    public long incTweetCount(String user) throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);

        long ret = users.incrementColumnValue(Bytes.toBytes(user),
                INFO_FAM,
                TWEETS_COL,
                1L);

        users.close();
        return ret;
    }

    public static Put makePut(String username, byte[] fam, byte[] qual, byte[] val) {
        Put p = new Put(Bytes.toBytes(username));
        p.add(fam, qual, val);
        return p;
    }

    private static class User extends AbstractUser {
        private User(Result r) {
            this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL), r.getValue(INFO_FAM, EMAIL_COL),
                    r.getValue(INFO_FAM, PASS_COL), r.getValue(INFO_FAM, TWEETS_COL));
        }

        private User(byte[] user, byte[] name, byte[] email, byte[] password, byte[] tweetCount) {
            this(Bytes.toString(user), Bytes.toString(name), Bytes.toString(email), Bytes.toString(password));
            this.tweetCount = Bytes.toLong(tweetCount);
        }

        private User(String user, String name, String email, String password) {
            this.user = user;
            this.name = name;
            this.email = email;
            this.password = password;
            this.tweetCount = 0;
        }
    }
}
