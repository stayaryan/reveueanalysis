import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import java.io.IOException;

public class MonthRevenueRanking {
        public static class MonthRevenueMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
                IntWritable yearMonth = new IntWritable();
                DoubleWritable revenue = new DoubleWritable();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String[] columns = value.toString().split(",");
                        if (columns.length == 3) {
                                int arrivalYear = Integer.parseInt(columns[0]);
                                int arrivalMonth = Integer.parseInt(columns[1]);
                                double totalRevenue = Double.parseDouble(columns[2]);

                                yearMonth.set(arrivalYear * 100 + arrivalMonth);
                                revenue.set(totalRevenue);

                                context.write(yearMonth, revenue);
                        }
                }
        }

        public static class MonthRevenueReducer extends Reducer<IntWritable, DoubleWritable, Text, NullWritable> {
                TreeMap<Double, IntWritable> revenueMap;

                protected void setup(Context context) {
                        revenueMap = new TreeMap<>(Collections.reverseOrder());
                }

                public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                                throws IOException, InterruptedException {
                        Double totalRevenue = 0.0;
                        for (DoubleWritable value : values) {
                                totalRevenue += value.get();
                        }
                        revenueMap.put(totalRevenue, new IntWritable(key.get()));
                        totalRevenue = 0.0;
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {
                        int count = 1;
                        for (Map.Entry<Double, IntWritable> entry : revenueMap.entrySet()) {
                                double revenue = entry.getKey();
                                IntWritable month = entry.getValue();
                                String output = "RANK " + count + "," + month.toString() + "," + revenue;
                                count = count + 1;
                                context.write(new Text(output), NullWritable.get());
                        }
                }
        }

        public static class ECSeasonMap extends Mapper<Object, Text, IntWritable, DoubleWritable> {
                int[] arr = new int[4];

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String[] columns = value.toString().split(",");
                        if (columns.length == 3) {
                                double rev = Double.parseDouble(columns[2]);
                                int year_mon = Integer.parseInt(columns[1]);
                                int year = year_mon / 100;
                                if (arr[year - 2015] == 0) {
                                        arr[year - 2015] = -1;
                                        context.write(new IntWritable(year_mon), new DoubleWritable(rev));
                                }
                        }

                }
        }

        public static class ECSeasonReduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
                TreeMap<DoubleWritable, IntWritable> revenueMap;

                protected void setup(Context context) {
                        revenueMap = new TreeMap<>(Collections.reverseOrder());
                }

                public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                                throws IOException, InterruptedException {
                        double totalRevenue = 0.0;
                        for (DoubleWritable value : values) {
                                totalRevenue = value.get();
                        }
                        revenueMap.put(new DoubleWritable(totalRevenue), new IntWritable(key.get()));
                }

                protected void cleanup(Context context) throws IOException, InterruptedException {

                        for (Map.Entry<DoubleWritable, IntWritable> entry : revenueMap.entrySet()) {
                                context.write(entry.getValue(), entry.getKey());
                        }
                }

        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "Month Revenue Ranking");

                job1.setJarByClass(MonthRevenueRanking.class);
                job1.setMapperClass(MonthRevenueMapper.class);
                job1.setReducerClass(MonthRevenueReducer.class);
                job1.setOutputKeyClass(IntWritable.class);
                job1.setOutputValueClass(DoubleWritable.class);

                FileInputFormat.addInputPath(job1, new Path(args[0]));
                Path intermediateOutputPath = new Path(args[1], "intermediate_output");
                FileOutputFormat.setOutputPath(job1, intermediateOutputPath);
                boolean job1Completed = job1.waitForCompletion(true);
                if (job1Completed) {
                        Job job2 = Job.getInstance(conf, "Revenue Sorting");

                        job2.setJarByClass(MonthRevenueRanking.class);
                        job2.setMapperClass(ECSeasonMap.class);
                        job2.setReducerClass(ECSeasonReduce.class);
                        job2.setOutputKeyClass(IntWritable.class);
                        job2.setOutputValueClass(DoubleWritable.class);

                        FileInputFormat.addInputPath(job2, intermediateOutputPath);
                        FileOutputFormat.setOutputPath(job2, new Path(args[1], "final_output"));
                        System.exit(job2.waitForCompletion(true) ? 0 : 1);

                }
        }
}
