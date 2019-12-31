package count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// mapper <입력 키, 입렵 값, 출력키, 출력값>
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	// 숫자 1을 직접 사용할수 없음
	private final static IntWritable one=new IntWritable(1);
	private Text word=new Text();
	//라인번호 텍스트형식의 데이터를 읽어서 ==> 단어 카운트 형식으로 변환
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer st = new StringTokenizer(value.toString());
		while(st.hasMoreElements()) {//다음요소가 있으면
			word.set(st.nextToken());//다음요소를 읽어서 key로 설정
			// key, value형식으로 저장(read,1 a,1 book,1)
			context.write(word, one);
		}
	}

}
