package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
keyin--k2新
value--v2
keyout--k3
value--v3
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    //新k2,v2-->k3,v3;k3,v3写入上下文context
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //key--新k2; value 集合  新v2;context   上下文对象
        //新k2,v2------>k3,v3:集合相加;k3,v3写入上下文;

    long count=0;
    for(LongWritable value:values){
        count+=value.get();
    }
    context.write(key,new LongWritable(count));
    }
}
