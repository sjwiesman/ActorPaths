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

        /**
         * Maps each path of tab delimited values to its first and last elements
         * Example:
         * value = "a b c"
         * mapping = "a" -> "a b c" and "c" -> "a b c"
         * @param key The line number of the value
         * @param value A path of tab delimited actors
         * @param context The ouput
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * @param list A list of strings
         * @return true if the list contains duplicates, false otherwise
         */
        public boolean duplicates(List<String> list) {
            Set<String> set = new HashSet<String>(list);
            return !(set.size() == list.size());
        }

        /**
         * @param list A list of strings
         * @return The values of the string combined, tab delimited
         */
        public String buildString(List<String> list) {

            final StringBuilder builder = new StringBuilder(list.get(0));
            for (int i = 1; i < list.size(); i++) {
                builder.append('\t');
                builder.append(list.get(i));
            }

            return builder.toString();
        }

        /**
         * A simple class to hold a pair of list of strings
         */
        public static class Pair {
            public final List<String> head;
            public final List<String> tail;

            public Pair(List<String> head, List<String> tail) {
                this.head = head;
                this.tail = tail;
            }
        }

        /**
         * Partition iterates over the iterable collection and separates values
         * based on if the key exists at the beginning or end of string
         * @param key Some key that either exists at the beginning or end of each string
         * @param iterable An object that contains an iterator over a collection of Text objects
         * @return A pair of the partitioned lists
         */
        public Pair partition(String key, Iterable<Text> iterable) {

            final Iterator<Text> it = iterable.iterator();
            final List<String> head = new ArrayList<String>();
            final List<String> tail = new ArrayList<String>();

            while (it.hasNext()) {
                final String value = it.next().toString();
                if (value.startsWith(key)) {
                    head.add(value);
                } else {
                    tail.add(value);
                }
            }

            return new Pair(head, tail);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //Partition each of the values depending on if the key exists in its head or tail
            final Pair pair = partition(key.toString(), values);

            for (String back : pair.head) {

                //Remove the key from the values
                //We will use the key in the corresponding array
                final List<String> backActors = new ArrayList<String>(Arrays.asList(back.split("\t")));
                backActors.remove(0);

                for (String front : pair.tail) {

                    final List<String> frontActors = new ArrayList<String>(Arrays.asList(front.split("\t")));

                    //Check that the path that will be created is of the appropriate size
                    if (frontActors.size() + backActors.size() != pathLength) {
                        continue;
                    }

                    //Combine the two lists
                    final List<String> path = new ArrayList<String>(frontActors);
                    path.addAll(backActors);

                    //Skip this list if it contains duplicates
                    //ex "c b c"
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

        //Determine the length of the path to be created
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
