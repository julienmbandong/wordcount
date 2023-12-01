package andeme.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * This method is called once for each key.
     * Most applications will define their reduce class by overriding this method.
     * The default implementation is an identity function.
     * 
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val: values) {
            System.out.println("Value = "+val.get());
            sum += val.get();
        }
        System.out.println("-->Sum = "+sum);
        result.set(sum);
        context.write(key, result);
    }
}
