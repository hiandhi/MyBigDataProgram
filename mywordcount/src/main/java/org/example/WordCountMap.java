package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
四个泛型:keyin--k1;valuein--v1;
keyout--k2;
 */
public class WordCountMap extends Mapper <LongWritable, Text,Text, LongWritable>{      //有java类型不用,就是玩

    @Override   //map方法将k1,v1转为k2,v2
    /*
    key--k1     行偏移量
    value--v1   textData
    context     上下文对象,给suffering
     */
    /*
    如何将k1,v1转为k2,v2
    0   hello,world,hadoop
    15 hafs,hive,hello
    <--------------------------------->
    k2                     v2
    hello                   1
    hdfs                    1
     */
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String[] split=value.toString().split(",");           //拆分textdata,遍历数组,组装k2,v2,将k2,v2写入上下文
        Text text =new Text();
        LongWritable lo=new LongWritable();

        for(String word:split){
            //context.write(new Text(word),new LongWritable(1));
            text.set(word);lo.set(1);
            context.write(text,lo);
        }
    }

}