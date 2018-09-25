package com.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    public static final String LEFT_FILENAME = "student_info.txt";
    public static final String RIGHT_FILENAME = "student_class_info.txt";
    public static final String LEFT_FILENAME_FLAG = "l";
    public static final String RIGHT_FILENAME_FLAG = "r";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();
        List<String> studentClassNames = new ArrayList<String>();
        String studentName = "";

        while (iterator.hasNext()) {
            String[] infos = iterator.next().toString().split("\t");
            //判断该条记录来自哪个文件，并根据文件格式解析记录获取相应的信息
            if (infos[1].equals(LEFT_FILENAME_FLAG)) {
                studentName = infos[0];
            } else if (infos[1].equals(RIGHT_FILENAME_FLAG)) {
                studentClassNames.add(infos[0]);
            }
        }

        //求笛卡儿积
        for (int i = 0; i < studentClassNames.size(); i++) {
            context.write(new Text(studentName), new Text(studentClassNames.get(i)));

        }

    }
}
