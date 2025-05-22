import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ConditionalProbability {

    public static class TransactionMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] products = value.toString().split(" ");
            Set<String> uniqueProducts = new HashSet<>(Set.of(products));

            for (String A : uniqueProducts) {
                for (String B : uniqueProducts) {
                    if (!A.equals(B)) {
                        pair.set(A + "," + B);
                        context.write(pair, one);
                    }
                }
                pair.set(A + ",*");
                context.write(pair, one);
            }
        }
    }

    public static class ProbabilityReducer extends Reducer<Text, IntWritable, Text, Text> {
        private int totalA = 0;
        private String currentA = "";

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            String[] parts = key.toString().split(",");
            String A = parts[0];
            String B = parts[1];

            if (!A.equals(currentA)) {
                totalA = 0;
                currentA = A;
            }

            if (B.equals("*")) {
                totalA = sum;
            } else {
                double probability = (double) sum / totalA;
                context.write(key, new Text(String.format("%.4f", probability)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Conditional Probability");
        job.setJarByClass(ConditionalProbability.class);
        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(ProbabilityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
