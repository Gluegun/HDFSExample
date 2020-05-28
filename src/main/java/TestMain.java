import java.io.IOException;
import java.util.List;

public class TestMain {
    public static void main(String[] args) throws IOException {

        FileAccess fileAccess = new FileAccess("ab89c2a3df9c");

//        fileAccess.create("/test/text.txt");

        fileAccess.append("/test/text.txt", "some content some content more");

        String read = fileAccess.read("/test/text.txt");
        System.out.println(read);

        fileAccess.delete("/test/text.txt");
        fileAccess.create("/test/text1.txt");
        fileAccess.create("/test/text2.txt");


        List<String> list = fileAccess.list("/test/");
        for (String s : list) {
            System.out.println(s);
        }

        System.out.println(fileAccess.isDirectory("/test/"));
        System.out.println(fileAccess.isDirectory("/test/text1.txt"));

    }
}
