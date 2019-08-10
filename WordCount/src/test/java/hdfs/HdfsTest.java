package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class HdfsTest {
    Configuration con;
    FileSystem fs;

    @Before
    public void conn() throws IOException {
        con = new Configuration(true);
        fs = FileSystem.get(con);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }
    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/ooxx");
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        fs.mkdirs(path);
    }

    @Test
    public void uploadFile() throws IOException {
        Path path = new Path("/ooxx/upload.txt");
        FSDataOutputStream outputStream = fs.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(new File(this.getClass().getResource("/").getPath()+"upload.txt")));
        byte[] bytes = new byte[100];
        while (in.read(bytes)!=-1){
            outputStream.write(bytes);
        }
        in.close();
        outputStream.close();
    }

    @Test
    public void downloadFile() throws IOException {
        Path path = new Path("/ooxx/upload.txt");
        FSDataInputStream in = fs.open(path);
        OutputStream out = new FileOutputStream(new File(this.getClass().getResource("/").getPath()+"/download.txt"));
        IOUtils.copyBytes(in,out,100);
    }

    @Test
    public void blockTest() throws IOException {
        Path path = new Path("/ooxx/test1.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
//        for (BlockLocation fileBlockLocation : fileBlockLocations) {
//            System.out.println("fileBlockLocation = " + fileBlockLocation);
//        }
        FSDataInputStream inputStream = fs.open(path);
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        inputStream.seek(1048576l);

        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
        System.out.println((char)inputStream.readByte());
    }
}
