package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    @Override   //指定job任务,创建job任务;配置job任务对象
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "WordCount");//创建任务
        job.setJarByClass(JobMain.class);//jar包运行不成功设置主类
        //配置job任务对象(8个)
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("C:\\Users\\Administrator\\Desktop\\wordcount.txt"));//读取文件的路径
        //第二步:Map阶段处理和数据类型
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);//设置Map阶段k2的类型
        job.setMapOutputValueClass(LongWritable.class);
        //suffering阶段,默认方式
        //第七步,Reduce阶段和k3,v3类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //第8步,设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path("file:///C:\\Users\\Administrator\\Desktop\\out");


        //设置输出路径
        TextOutputFormat.setOutputPath(job,path);//file:///表示本地运行,hdfs://在集群上
        //获取文件系统filesystem
        FileSystem fileSystem = FileSystem.get(new URI("file:///"), new Configuration());
        boolean exists = fileSystem.exists(path);
        if(exists){
            fileSystem.delete(path,true);//第二个参数递归删除
        }


        //等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl?0:1;//true正常返回0
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //启动Job任务
        int run = ToolRunner.run(conf, new JobMain(), args);//0成功
        System.exit(run);


    }
}
