package hbaseinaction;

import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 1:25 PM
 */
public class UsersTool {

    public static final String usage =
            "usertool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  add user name email password - add a new user.\n" +
                    "  get user - retrieve a specific user.\n" +
                    "  list - list all installed users.\n";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }

        HTablePool pool = new HTablePool();
        UsersDAO dao = new UsersDAO(pool);

        if ("get".equals(args[0])) {
            System.out.println("Getting user " + args[1]);
            AbstractUser user = dao.getUser(args[1]);
            System.out.println(user);
        }

        if ("add".equals(args[0])) {
            System.out.println("Adding user...");
            dao.addUser(args[1], args[2], args[3], args[4]);
            AbstractUser user = dao.getUser(args[1]);
            System.out.println("Successfully added user " + user);
        }

        if ("list".equals(args[0])) {
            for (AbstractUser u : dao.getUsers()) {
                System.out.println(u);
            }
        }

        pool.closeTablePool(UsersDAO.TABLE_NAME);
    }
}
