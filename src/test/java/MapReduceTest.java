/**
 * @author by sethwiesman on 12/4/14.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapReduceTest {

    private Mapper mapper = new ActorPaths.Map();
    private ActorPaths.Reduce reducer = new ActorPaths.Reduce();

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;
    private MapReduceDriver<Text, Text, Text, Text, Text, NullWritable> mapReducedriver;

    @Before
    public void setup() {
        mapDriver  = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReducedriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }
    @Test
    public void properMap() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("a\tb"))
                .withOutput(new Text("a"), new Text("a\tb"))
                .withOutput(new Text("b"), new Text("a\tb"))
                .runTest();
    }

    @Test
    public void mapWithTrailingWhiteSpace() {
        mapDriver.withInput(new LongWritable(), new Text("    a\tb      "))
                .withOutput(new Text("a"), new Text("a\tb"))
                .withOutput(new Text("b"), new Text("a\tb"))
                .runTest();
    }

    @Test
    public void containsDuplicates() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("a");
        list.add("c");

        assertTrue("Did not find duplicates where they exist", reducer.duplicates(list));
    }

    @Test
    public void containsNoDuplicates() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");

        assertFalse("Found duplicates where they don't exist", reducer.duplicates(list));
    }

    @Test
    public void buildString() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");

        assertEquals("Failed to build output string", "a\tb\tc", reducer.buildString(list));
    }

    @Test
    public void partition() {
        List<Text> list = new ArrayList<Text>();
        list.add(new Text("a\tb"));
        list.add(new Text("b\ta"));

        final ActorPaths.Reduce.Pair pair = reducer.partition("a", list);
        assertEquals("Multiple front overlaps", 1, pair.head.size());
        assertEquals("Wrong head item", "a\tb", pair.head.get(0));
        assertEquals("Multiple tails", 1, pair.tail.size());
        assertEquals("Wrong tails item", "b\ta", pair.tail.get(0));
    }

    @Test
    public void reducer() {
        List<Text> list = new ArrayList<Text>();
        list.add(new Text("a\tb"));
        list.add(new Text("b\ta"));
        list.add(new Text("b\tc"));
        list.add(new Text("c\tb"));

        reduceDriver.withInput(new Text("b"), list);
        reduceDriver.withOutput(new Text("c\tb\ta"), NullWritable.get());
        reduceDriver.withOutput(new Text("a\tb\tc"), NullWritable.get());

        reduceDriver.runTest();
    }

}
