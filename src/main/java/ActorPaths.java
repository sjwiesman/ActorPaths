import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * @author by sethwiesman on 12/4/14.
 */
public class ActorPaths {

    private static int pathLength = 0;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        protected boolean duplicates(List<String> list) {
            for (int i = 0; i < list.size(); i++) {
                for (int j = i+1; j < list.size(); j++) {
                    if (list.get(i).equals(list.get(j))) {
                        return false;
                    }
                }
            }

            return true;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] path = value.toString().trim().split("\t");

            if (path.length < 2) {
                return;
            }

            if (duplicates(Arrays.asList(path))) {
                return;
            }

            //Key each value based on the starting and ending actors in the current path
            //example
            //path = {"a", "b", "c"}
            //key on "a" and "c"

            context.write(new Text(path[0]), value);
            context.write(new Text(path[path.length-1]), value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        protected boolean duplicates(List<String> list) {
            for (int i = 0; i < list.size(); i++) {
                for (int j = i+1; j < list.size(); j++) {
                    if (list.get(i).equals(list.get(j))) {
                        return false;
                    }
                }
            }

            return true;
        }

        private String buildString(List<String> list) {

            final StringBuilder builder = new StringBuilder(list.get(0));
            for (int i = 1; i < list.size(); i++) {
                builder.append('\t');
                builder.append(list.get(i));
            }

            return builder.toString();
        }

        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {

            final String overlap = key.toString();
            final List<String> head = new ArrayList<String>();
            final List<String> tail = new ArrayList<String>();

            while (values.hasNext()) {
                final String value = values.next().toString();
                if (value.startsWith(overlap)) {
                    head.add(value);
                } else {
                    tail.add(value);
                }
            }


            for (String back : head) {

                final List<String> backActors = new ArrayList<String>(Arrays.asList(back.split("\t")));
                backActors.remove(0);

                for (String front : tail) {

                    final String reverseBack = new StringBuilder(back).reverse().toString();

                    if (reverseBack.equals(front)) {
                        continue;
                    }

                    final List<String> frontActors = new ArrayList<String>(Arrays.asList(front.split("\t")));

                    final List<String> path = new ArrayList<String>(frontActors);
                    path.addAll(backActors);

                    if (path.size() != pathLength) {
                        continue;
                    }

                    if (duplicates(path)) {
                        continue;
                    }

                    final String string = buildString(path);

                    context.write(new Text(string), NullWritable.get());

                }
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <path length> <in dir> <out dir>");
        }

        pathLength = Integer.parseInt(args[0]);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "actor path " + pathLength);
        job.setJarByClass(ActorPaths.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
