package hof;

import java.util.ArrayList;
import java.util.List;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/9/13
 * Time: 10:47 PM
 */
public class FunctionLib {
    // map :: (a -> b) -> [a] -> [b]
    public static <A, B> List<B> map(IFunction<A, B> fun, List<A> args) {
        List<B> results = new ArrayList<B>();
        for (A a : args)
            results.add(fun.call(a));
        return results;
    }

    // zip :: [a] -> [b] -> [(a, b)]
    public static <A, B> List<Pair<A, B>> zip(List<A> listA, List<B> listB) {
        List<Pair<A, B>> results = new ArrayList<Pair<A, B>>();
        int len = Math.min(listA.size(), listB.size());
        for (int i = 0; i < len; i++) {
            results.add(new Pair<A, B>(listA.get(i), listB.get(i)));
        }
        return results;
    }

    // filter :: (a -> bool) -> [a] -> [a]
    public static <A> List<A> filter(IFunction<A, Boolean> pred, List<A> listA) {
        List<A> results = new ArrayList<A>();
        for (A a : listA) {
            if (pred.call(a))
                results.add(a);
        }
        return results;
    }

    // foldl1 :: (a -> a -> a) -> [a] -> a
    // In Haskell the function type operator -> is right associative,
    // So, the function description "a -> a -> a" is parsed as "a -> (a -> a)"
    // This order of application is represented in the nesting of IFunction's below
    public static <A> A foldl1(IFunction<A, IFunction<A, A>> fn, List<A> listA) {
        A accumulator = null;
        if (listA.size() == 0)
            return accumulator;

        accumulator = listA.get(0);
        if (listA.size() == 1)
            return accumulator;

        for (int i = 1; i < listA.size(); i++) {
            // currying means two .call invocations
            accumulator = fn.call(accumulator).call(listA.get(i));
        }
        return accumulator;
    }
}