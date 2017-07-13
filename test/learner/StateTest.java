import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
自己的wordcount改良
可以去除不想要的字元 並且將輸入進來的string 分開讀取成char 
計算字母出現次數
*/

public class WordCount3 {
   
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
      
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text("I dont want it");
    private static String leng = new String();
    private static int len ;
    private static char ch ;
    private static String used = new String("I dont want it");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        leng = itr.nextToken();
        //leng.set(itr.nextToken());
         len = leng.length();
         //word = used ;
         for(int i=0;i<len;i++)
         {
            //used = leng[i];
            ch = leng.charAt(i);
            //word = leng.charAt(i);
            //word(char ch);
            //word.set(used);
          if(ch=='I')
          {
             context.write(word, one);
            continue;
          }
           context.write(new Text(String.valueOf(ch)), one);

            //context.write(word, one);
         }
           /*
        word.set(itr.nextToken());
        context.write(word, one);
           */
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
