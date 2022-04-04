package com.zpx;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class fff{
    public void testMkdir() throws URISyntaxException,IOException,InterruptedException{
        //连接的集群的NameNode的地址
        URI uri = new URI("hdfs://node2:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        //获取到客户端对象

        //设置操作hdfs的用户
        String user = "fzl";

        FileSystem fileSystem = FileSystem.get(uri, configuration,user);

        //执行相关命令操作
        fileSystem.mkdirs(new Path("/xiyouji/huaguoshan"));
        //关闭资源
        fileSystem.close();
    }


}