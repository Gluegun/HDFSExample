import java.io.IOException;
import java.util.List;

public class TestMain {
    public static void main(String[] args) throws IOException {

        FileAccess fileAccess = new FileAccess("1e69e151014c");

        String filePath = "/test/text.txt";

        fileAccess.create(filePath);

        fileAccess.append(filePath, "some content");
        fileAccess.append(filePath, " and a bit more content");
        fileAccess.append(filePath, " and last one just to test");

        String read = fileAccess.read(filePath);
        System.out.println(read);

        fileAccess.delete(filePath);

        fileAccess.create("/test/text1.txt");
        fileAccess.create("/test/text2.txt");

        List<String> list = fileAccess.list("/test/");
        for (String s : list) {
            System.out.println(s);
        }

        System.out.println(fileAccess.isDirectory("/test/"));
        System.out.println(fileAccess.isDirectory("/test/text1.txt"));


        fileAccess.append("/test/text3.txt", "some content for nonexisted file text3.txt");

        System.out.println(fileAccess.read("/test/text3.txt"));


    }
}
