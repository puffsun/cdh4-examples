package hof;

/**
 * Replace this line with class description.
 * <p/>
 * User: George Sun
 * Date: 7/9/13
 * Time: 10:57 PM
 */
public class Pair<A, B> {
    public A first;
    public B second;

    Pair(A a, B b) {
        first = a;
        second = b;
    }

    public static <A, B> IFunction<Pair<A, B>, A> firstFunction(Class<A> a, Class<B> b) {
        return new IFunction<Pair<A, B>, A>() {
            public A call(Pair<A, B> arg) {
                return arg.first;
            }
        };
    }

    public static <A, B> IFunction<Pair<A, B>, B> secondFunction(Class<A> a, Class<B> b) {
        return new IFunction<Pair<A, B>, B>() {
            public B call(Pair<A, B> arg) {
                return arg.second;
            }
        };
    }

    @Override
    public String toString() {
        String firstStr = first != null ? first.toString() : "null";
        String secondStr = second != null ? second.toString() : "null";
        return "(" + firstStr + ", " + secondStr + ")";
    }
}