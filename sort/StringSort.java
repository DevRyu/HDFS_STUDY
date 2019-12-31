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
		//�⺻ mapper, �Է� ���ڵ尡 �״�� ��� ���ڵ尡 ��
		job.setMapperClass(Mapper.class);
		//�⺻ reducer, ���� ��·��ڵ尡 �״�� ���ེ�� ����� ��
		//reduce �ܰ迡�� ���������� sort�� ó����
		job.setReducerClass(Reducer.class);
		
		//�� ��°� ���ེ ����� key,value �ڷ���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//reduce�� ���� 1�� ����(���ڵ� ������ �پ���� ����)
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//�Է� ������ ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//��� ���丮 ���� - ���������� ��������
		SequenceFileOutputFormat.setOutputPath(
				job, new Path(args[1]));
		// ��� ���� ����(60% ���� ������ ����)
		SequenceFileOutputFormat.setOutputCompressionType(
				job, SequenceFile.CompressionType.BLOCK);
		
		job.waitForCompletion(true);
	}
}









