
package count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class wordcount {
	public static void main(String[] args) 
			throws Exception {
		Configuration conf=new Configuration();
		if(args.length != 2){
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}
		Job job=Job.getInstance(conf, "WordCount"); //job 이름
		job.setJarByClass(wordcount.class); // job class 이름
		job.setMapperClass(MyMapper.class); // mapper class
		job.setReducerClass(MyReducer.class); //reduce class
		//입출력 데이터 형식 지정
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		//출력키,값 데이터 형식 지정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//입력 파일, 출력 디렉토리 지정
		FileInputFormat.addInputPath(job, new Path(args[0])); //입력파일
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //출력파일
		job.waitForCompletion(true); //hadoop 분석 작업 진행
	}
}








