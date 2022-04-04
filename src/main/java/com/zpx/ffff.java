package com.zpx;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ffff {



    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://node1:8020");
        Configuration configuration = new Configuration();
        String user = "root";
        FileSystem fs = FileSystem.get(uri, configuration, user);

        fs.mkdirs(new Path("/xixixi/shhdh"));
        fs.close();


    }
}
