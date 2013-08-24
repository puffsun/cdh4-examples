package hbaseinaction;

import org.joda.time.DateTime;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/20/13
 * Time: 5:26 PM
 */
public abstract class AbstractTwit {

    public String user;
    public DateTime dt;
    public String text;

    @Override
    public String toString() {
        return String.format("<Twit: %s %s %s>", user, dt, text);
    }
}
