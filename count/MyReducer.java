package count;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// Reducer<InputKey,InputValue,OutputKey,OutputValue>
public class MyReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result=new IntWritable();
// key, list => key, value로 합치는 과정	 a,(1,1) => a,2
	@Override
	public void reduce(
			Text key, Iterable<IntWritable> values,Context context)
					throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable val : values){ // value list(1,1
			sum += val.get(); //리스트를 합산하는 과정
		}
		result.set(sum); //출력값 설정
		context.write(key, result);  //출력 데이터 생성
	}
}










