import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MarketPriceAnalysis {

    public static class PriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable price = new DoubleWritable();
        private Text item = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replace("\"", ""); // Remove quotes
            String[] fields = line.split(",");

            // Skip header row
            if (line.startsWith("ten,gia")) return; 

            // Ensure there are enough columns
            if (fields.length < 4) return;

            String itemName = fields[0].trim();
            String date = fields[3].trim().split(" ")[0]; // Extract only YYYY-MM-DD
            double itemPrice;

            try {
                itemPrice = Double.parseDouble(fields[1].trim());
            } catch (NumberFormatException e) {
                return; // Skip invalid rows
            }

            price.set(itemPrice);

            // Task (a): Count items with price > threshold
            double threshold = Double.parseDouble(context.getConfiguration().get("price.threshold", "50000"));
            if (itemPrice > threshold) {
                context.write(new Text("HighPrice_" + date), new DoubleWritable(1));
            }

            // Task (b) & (c): Average, max, and min price per item
            item.set(itemName);
            context.write(item, price);
        }
    }

    public static class PriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            double maxPrice = Double.MIN_VALUE;
            double minPrice = Double.MAX_VALUE;

            for (DoubleWritable val : values) {
                double price = val.get();
                sum += price;
                count++;
                if (price > maxPrice) maxPrice = price;
                if (price < minPrice) minPrice = price;
            }

            if (key.toString().startsWith("HighPrice_")) {
                context.write(key, new Text(String.valueOf(count))); // Output for task (a)
            } else {
                double avgPrice = sum / count;
                context.write(key, new Text(String.format("Avg: %.2f, Max: %.2f, Min: %.2f", avgPrice, maxPrice, minPrice))); // Output for tasks (b) & (c)
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("price.threshold", args[2]); // Pass threshold value

        Job job = Job.getInstance(conf, "Market Price Analysis");
        job.setJarByClass(MarketPriceAnalysis.class);
        job.setMapperClass(PriceMapper.class);
        job.setReducerClass(PriceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

