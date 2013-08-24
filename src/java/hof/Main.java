package hof;

import java.util.ArrayList;
import java.util.List;

/**
 * Replace this line with class description.
 * <p/>
 * AbstractUser: George Sun
 * Date: 7/9/13
 * Time: 10:45 PM
 */
public class Main {
    public static void main(String[] args) {
        List<String> words = new ArrayList<String>();
        words.add("File");
        words.add("Edit");
        words.add("Source");
        words.add("Refactor");
        words.add("Navigate");
        words.add("Search");
        words.add("Project");
        words.add("Run");
        words.add("Window");
        words.add("Help");

        System.out.println("Starting With: ");
        FunctionLib.map(new PrintFunction<String>(), words);
        System.out.println("\n");

        System.out.println("The length of each String is: ");
        IFunction<String, Integer> lengthFunction = new IFunction<String, Integer>() {
            public Integer call(String arg) {
                return arg.length();
            }
        };

        List<Integer> lengths = FunctionLib.map(lengthFunction, words);
        FunctionLib.map(new PrintFunction<Integer>(), lengths);
        System.out.println("\n");

        System.out.println("The words with length bigger than 5 are:");
        List<Pair<String, Integer>> associations = FunctionLib.zip(words, lengths);
        IFunction<Pair<String, Integer>, Boolean> biggerThanFive = new IFunction<Pair<String, Integer>, Boolean>() {
            public Boolean call(Pair<String, Integer> arg) {
                return arg.second > 5;
            }
        };
        List<Pair<String, Integer>> longerThanFive = FunctionLib.filter(biggerThanFive, associations);

        List<String> stringsLongerThanFive = FunctionLib.map(Pair.firstFunction(String.class, Integer.class), longerThanFive);
        FunctionLib.map(new PrintFunction<String>(), stringsLongerThanFive);
        System.out.println("\n");

        System.out.println("When we concatenate these we get:");
        String concat = FunctionLib.foldl1(
                new IFunction<String, IFunction<String, String>>() {
                    public IFunction<String, String> call(final String arg1) {
                        return new IFunction<String, String>() {
                            public String call(String arg2) {
                                return arg1 + arg2;
                            }
                        };
                    }
                },
                stringsLongerThanFive);
        System.out.println(concat);
        System.out.println("\n");
    }
}
