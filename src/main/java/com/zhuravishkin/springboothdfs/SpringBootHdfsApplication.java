package com.zhuravishkin.springboothdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class SpringBootHdfsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHdfsApplication.class, args);
        Configuration conf = new Configuration();
        String uri = "hdfs://localhost:9000/";
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(uri), conf);
            System.out.println(fileSystem.getScheme());
            System.out.println(fileSystem.getCanonicalServiceName());
            System.out.println(fileSystem.getHomeDirectory());
            System.out.println(fileSystem.getWorkingDirectory());
            fileSystem.mkdirs(new Path(fileSystem.getWorkingDirectory() + "/app_dir"));
//            fileSystem.deleteOnExit(new Path(fileSystem.getWorkingDirectory() + "/app_dir"));
            fileSystem.createNewFile(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"));
//            fileSystem.deleteOnExit(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"));
            System.out.println(fileSystem
                    .exists(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt")));
            FSDataOutputStream outputStream = fileSystem
                    .create(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"));
            outputStream.writeChars("Hello World!\nHadoop\n");
//            fileSystem.truncate(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"), 0);
            outputStream.close();
            FSDataInputStream inputStream = fileSystem
                    .open(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"));
            String string = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            System.out.println(string);
            inputStream.close();
            fileSystem.copyToLocalFile(new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"),
                    new Path("test.txt"));
//            fileSystem.copyFromLocalFile(new Path("test.txt"),
//                    new Path(fileSystem.getWorkingDirectory() + "/app_dir/test.txt"));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
