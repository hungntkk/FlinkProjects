package MapReduceFlink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws IOException, URISyntaxException
    {
        Map<Integer, String> hMapNumbers = new HashMap<Integer, String>();
        hMapNumbers.put(1, "One");
        hMapNumbers.put(2, "Two");
        hMapNumbers.put(3, "Three");
        String b = "a: "+hMapNumbers.toString();

        System.out.println(b);

    }
}
