package hbaseinaction;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/20/13
 * Time: 12:49 PM
 */
public abstract class AbstractUser {

    public String user;
    public String name;
    public String email;
    public String password;
    public long tweetCount;

    @Override
    public String toString() {
        return String.format("<AbstractUser: %s, %s, %s, %s>", user, name, email, tweetCount);
    }
}
