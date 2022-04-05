package com.zpx;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class test {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://node1:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        String user = "root";
        fs = FileSystem.get(uri, configuration, user);
        //
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testmkdir() throws  IOException{
        fs.mkdirs(new Path("/xxi/sdh"));
    }

    //参数优先级
    //hdsf-default.xml<hdfs-site.xml<在项目资源目录下面的配置文件<代码里面的配置
    @Test
    public void testput() throws IOException {
        //参数1，删除原数据
        //参数二，是否允许覆盖，参数三原数据路径，参数四，目的地路径
        fs.copyFromLocalFile(false, true, new Path("D:\\hhh.txt"), new Path("hdfs://node1/xxi"));
    }

    @Test
    public void testget() throws IOException {
        //参数一源文件是否删除，参数二源文件路径hdfs，目标地址路径，奇偶校验
        fs.copyToLocalFile(false, new Path("/xxi"), new Path("D:\\"), true);
    }
    @Test
    public void testrm() throws IOException {
        //参数一要删除的路径，参数二；是否递归删除
        fs.delete(new Path("/xffxi"),true);
        //删除空目录
    }
    @Test
    public void testmv() throws IOException {
        fs.rename(new Path("/xxi/hhh.txt"), new Path("/xxi/ss.txt"));
    }
}
