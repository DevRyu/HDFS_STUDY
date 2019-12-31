package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "StringSort");
		job.setJarByClass(StringSort.class);
		//기본 mapper, 입력 레코드가 그대로 출력 레코드가 됨
		job.setMapperClass(Mapper.class);
		//기본 reducer, 맵의 출력레코드가 그대로 리듀스의 출력이 됨
		//reduce 단계에서 내부적으로 sort가 처리됨
		job.setReducerClass(Reducer.class);
		
		//맵 출력과 리듀스 출력의 key,value 자료형
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//reduce의 수를 1로 설정(레코드 갯수가 줄어들지 않음)
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//입력 데이터 파일
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//출력 디렉토리 지정 - 시퀀스파일 형식으로
		SequenceFileOutputFormat.setOutputPath(
				job, new Path(args[1]));
		// 블록 단위 압축(60% 정도 사이즈 감소)
		SequenceFileOutputFormat.setOutputCompressionType(
				job, SequenceFile.CompressionType.BLOCK);
		
		job.waitForCompletion(true);
	}
}









