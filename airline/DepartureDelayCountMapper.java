package airline;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String workType;
    private final static IntWritable outputValue = new IntWritable(1);
    private Text outputKey = new Text();
    
    // Mapper 생성될 때 한번만 실행 
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
    }
 
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
        if(workType.equals("departure")){
            if(parser.getDepartureDelayTime() > 0){
                outputKey.set(parser.getYear() + "." + parser.getMonth());
                context.write(outputKey, outputValue);
            }
        } else if(workType.equals("arrival")){
            if(parser.getArriveDelayTime() > 0){
                outputKey.set(parser.getYear() + "." + parser.getMonth());
                context.write(outputKey, outputValue);
            }
        } else {
            System.out.println("baaaaaaaaaaaaaaaaaaaaaaaaaaam!!!");
        }
    }
}
