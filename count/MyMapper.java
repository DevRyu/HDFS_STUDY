package count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// mapper <�Է� Ű, �Է� ��, ���Ű, ��°�>
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	// ���� 1�� ���� ����Ҽ� ����
	private final static IntWritable one=new IntWritable(1);
	private Text word=new Text();
	//���ι�ȣ �ؽ�Ʈ������ �����͸� �о ==> �ܾ� ī��Ʈ �������� ��ȯ
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer st = new StringTokenizer(value.toString());
		while(st.hasMoreElements()) {//������Ұ� ������
			word.set(st.nextToken());//������Ҹ� �о key�� ����
			// key, value�������� ����(read,1 a,1 book,1)
			context.write(word, one);
		}
	}

}
