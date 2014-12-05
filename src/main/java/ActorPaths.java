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

    private static int pathLength = 3;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String trimmedValue = value.toString().trim();
            final String[] path = trimmedValue.split("\t");

            if (path.length < 2) {
                return;
            }

            //Key each value based on the starting and ending actors in the current path
            //example
            //path = {"a", "b", "c"}
            //key on "a" and "c"

            context.write(new Text(path[0]), new Text(trimmedValue));
            context.write(new Text(path[path.length-1]), new Text(trimmedValue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        public boolean duplicates(List<String> list) {
            Set<String> set = new HashSet<String>(list);
            return !(set.size() == list.size());
        }

        public String buildString(List<String> list) {

            final StringBuilder builder = new StringBuilder(list.get(0));
            for (int i = 1; i < list.size(); i++) {
                builder.append('\t');
                builder.append(list.get(i));
            }

            return builder.toString();
        }

        public static class Pair {
            public final List<String> head;
            public final List<String> tail;

            public Pair(List<String> head, List<String> tail) {
                this.head = head;
                this.tail = tail;
            }
        }

        public Pair partition(String key, Iterable<Text> iterable) {

            final Iterator<Text> it = iterable.iterator();
            final List<String> head = new ArrayList<String>();
            final List<String> tail = new ArrayList<String>();

            while (it.hasNext()) {
                final String value = it.next().toString();
                if (value.split("\t")[0].equals(key)) {
                    head.add(value);
                } else {
                    tail.add(value);
                }
            }

            return new Pair(head, tail);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final Pair pair = partition(key.toString(), values);


            for (String back : pair.head) {

                final List<String> backActors = new ArrayList<String>(Arrays.asList(back.split("\t")));
                backActors.remove(0);

                for (String front : pair.tail) {

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
