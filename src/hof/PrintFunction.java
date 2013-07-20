package hof;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/9/13
 * Time: 10:47 PM
 */
public class PrintFunction<E> implements IFunction<E, Object> {

    @Override
    public Object call(E arg) {
        System.out.println(arg);
        return null;
    }
}