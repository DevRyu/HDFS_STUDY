
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
		Job job=Job.getInstance(conf, "WordCount"); //job �̸�
		job.setJarByClass(wordcount.class); // job class �̸�
		job.setMapperClass(MyMapper.class); // mapper class
		job.setReducerClass(MyReducer.class); //reduce class
		//����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		//���Ű,�� ������ ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//�Է� ����, ��� ���丮 ����
		FileInputFormat.addInputPath(job, new Path(args[0])); //�Է�����
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //�������
		job.waitForCompletion(true); //hadoop �м� �۾� ����
	}
}








