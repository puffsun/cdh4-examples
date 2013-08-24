package hbaseinaction;

import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 6:07 PM
 */

public class TwitsTool {

    public static final String usage =
            "twitstool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  post user text - post a new twit on user's behalf.\n" +
                    "  list user - list all twits for the specified user.\n";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }

        HTablePool pool = new HTablePool();
        TwitsDAO twitsDao = new TwitsDAO(pool);
        UsersDAO usersDao = new UsersDAO(pool);

        if ("post".equals(args[0])) {
            DateTime now = new DateTime();
            twitsDao.postTwit(args[1], now, args[2]);
            AbstractTwit t = twitsDao.getTwit(args[1], now);
            usersDao.incTweetCount(args[1]);
            System.out.println("Successfully posted " + t);
        }

        if ("list".equals(args[0])) {
            List<AbstractTwit> twits = twitsDao.list(args[1]);
            for (AbstractTwit t : twits) {
                System.out.println(t);
            }
        }

        pool.closeTablePool(TwitsDAO.TABLE_NAME);
    }
}

