import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FileAccess {

    private FileSystem hdfs;

    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param containerName - the path to the root of HDFS,
     *                      for example, hdfs://localhost:32771
     */
    public FileAccess(String containerName) {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        try {
            hdfs = FileSystem.get(
                    new URI("hdfs://" + containerName + ":8020"), configuration
            );
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) {
        Path hdfsPath = new Path(path);
        try {
            hdfs.create(hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {

        Path hdfsPath = new Path(path);
        FSDataOutputStream fileOutputStream = null;
        try {
            if (hdfs.exists(hdfsPath)) {
                fileOutputStream = hdfs.append(hdfsPath);
                fileOutputStream.writeBytes(content);
            } else {
                fileOutputStream = hdfs.create(hdfsPath);
                fileOutputStream.writeBytes(content);
            }
        } finally {

            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
        }

    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        String text = "";
        Path file = new Path(path);
        if (!hdfs.exists(file)) {
            throw new IOException("File not found");
        }
        byte[] buffer = new byte[256];
        try (FSDataInputStream inputStream = hdfs.open(file); OutputStream out = new ByteArrayOutputStream()) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            text = out.toString();

        } catch (IOException ex) {
            System.out.println("Error while copying file");
        }

        return text;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) {
        Path hdfsPath = new Path(path);
        try {
            hdfs.delete(hdfsPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        Path filePath = new Path(path);
        return hdfs.isDirectory(filePath);
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {
        Path filePath = new Path(path);
        List<String> fileList = new ArrayList<>();
        FileStatus[] fileStatus = hdfs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(list(fileStat.getPath().toString()));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }
}
