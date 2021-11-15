package Test;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String args[]) {
        List<String> languages= new ArrayList<>();

        // Add elements in the ArrayList
        languages.add("Java");
        languages.add("Python");
        languages.add("C");
        System.out.println("ArrayList: " + languages);

        // convert ArrayList to String
        String list = languages.toString();
        System.out.println("String: " + list);
    }
}
