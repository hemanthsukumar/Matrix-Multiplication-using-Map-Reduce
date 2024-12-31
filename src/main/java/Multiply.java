/* Hemanth Sukumar Vangala     UTA ID: 1002118951 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;

// Custom Writable class representing matrix elements
class Triple implements Writable {
    int type;        // 0 for matrix M, 1 for matrix N
    int columnIdx;  // Column index of the element
    double value;   // Element value

    // Default constructor
    public Triple() {
        this(0, 0, 0.0); // Using constructor chaining
    }
    // Parameterized constructor
    public Triple(int type, int index, double value) {
        this.type = type;
        this.columnIdx = index;
        this.value = value;
    }
    @Override
    public void readFields(DataInput input) throws IOException {
        type = input.readInt();
        columnIdx = input.readInt();
        value = input.readDouble();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(type);
        output.writeInt(columnIdx);
        output.writeDouble(value);
    }
    @Override
    public String toString() {
        return String.format("(Type: %d, Column: %d, Value: %.2f)", type, columnIdx, value);
    }
}

// Custom WritableComparable class to represent index pairs
class Pair implements WritableComparable<Pair> {
    int row;    // Row index
    int column; // Column index
    // Default constructor
    public Pair() {
        this(0, 0); // Constructor chaining
    }
    // Parameterized constructor
    public Pair(int row, int column) {
        this.row = row;
        this.column = column;
    }
    @Override
    public void readFields(DataInput input) throws IOException {
        row = input.readInt();
        column = input.readInt();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(row);
        output.writeInt(column);
    }
    @Override
    public int compareTo(Pair other) {
        int rowComparison = Integer.compare(this.row, other.row);
        return (rowComparison != 0) ? rowComparison : Integer.compare(this.column, other.column);
    }
    @Override
    public String toString() {
        return String.format("%d %d", row, column); // Used String.format
    }
}

// Main class for multiplying matrices
public class Multiply {
    // Mapper for matrix M
    public static class M_Mapper extends Mapper<Object, Text, IntWritable, Triple> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            if (tokens.length == 3) {
                int columnIndex = Integer.parseInt(tokens[0]);
                double elementValue = Double.parseDouble(tokens[2]);

                IntWritable outputKey = new IntWritable(Integer.parseInt(tokens[1]));
                Triple element = new Triple(0, columnIndex, elementValue);

                // Reduced debugging statement
                if (context.getTaskAttemptID().getTaskID().getId() % 100 == 0) { // Print selectively every 100th task
                    System.out.printf("M_Mapper: Key=%d, Value=%s%n", outputKey.get(), element.toString());
                }

                context.write(outputKey, element);
            }
        }
    }

    // Mapper for matrix N
    public static class N_Mapper extends Mapper<Object, Text, IntWritable, Triple> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            if (tokens.length == 3) {
                int columnIndex = Integer.parseInt(tokens[1]);
                double elementValue = Double.parseDouble(tokens[2]);
                IntWritable outputKey = new IntWritable(Integer.parseInt(tokens[0]));
                Triple element = new Triple(1, columnIndex, elementValue);
                // Reduced debugging statement
                if (context.getTaskAttemptID().getTaskID().getId() % 100 == 0) { // Print selectively every 100th task
                    System.out.printf("N_Mapper: Key=%d, Value=%s%n", outputKey.get(), element.toString());
                }
                context.write(outputKey, element);
            }
        }
    }

    // Reducer for combining results
    public static class MxN_Reducer extends Reducer<IntWritable, Triple, Pair, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
            List<Triple> matrixM = new ArrayList<>();
            List<Triple> matrixN = new ArrayList<>();

            // Classify elements into M and N
            for (Triple element : values) {
                Triple copiedElement = ReflectionUtils.newInstance(Triple.class, context.getConfiguration());
                ReflectionUtils.copy(context.getConfiguration(), element, copiedElement);

                if (copiedElement.type == 0) {
                    matrixM.add(copiedElement);
                } else {
                    matrixN.add(copiedElement);
                }
            }

            // Perform multiplication for every element in M with each element in N
            for (Triple mElement : matrixM) {
                for (Triple nElement : matrixN) {
                    Pair outputKey = new Pair(mElement.columnIdx, nElement.columnIdx);
                    double product = mElement.value * nElement.value;

                    // Reduced debugging
                    if (matrixM.size() * matrixN.size() < 10) { // Only print if the size is small
                        System.out.printf("MxN_Reducer: Key=%s, Product=%.2f%n", outputKey.toString(), product);
                    }
                    context.write(outputKey, new DoubleWritable(product));
                }
            }
        }
    }

    // Mapper for final output
    public static class MxN_Map extends Mapper<Object, Text, Pair, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+"); // Split by any whitespace

            if (parts.length == 3) {
                Pair outputKey = new Pair(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(parts[2]));
                // Reduced debugging
                if (context.getTaskAttemptID().getTaskID().getId() % 100 == 0) {
                    System.out.printf("MxN_Map: Key=%s, Value=%.2f%n", outputKey.toString(), outputValue.get());
                }
                context.write(outputKey, outputValue);
            }
        }
    }

    // Reducer for final summation of results
    public static class MxN_Red extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.0;
            // Summing the values
            for (DoubleWritable value : values) {
                total += value.get();
            }
            // Reduced debugging
            if (total > 0.0) { // Only print if the result is meaningful
                System.out.printf("MxN_Red: Key=%s, Total=%.2f%n", key.toString(), total);
            }
            context.write(key, new DoubleWritable(total));
        }
    }
    // Main method to configure and execute the MapReduce jobs
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Multiply <input1> <input2> <temp_output> <final_output>");
            System.exit(2);
        }
        // First job: Multiply matrices
        Job job1 = Job.getInstance();
        job1.setJobName("Matrix Multiplication");
        job1.setJarByClass(Multiply.class);
        // Adding input paths for both matrices
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, M_Mapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, N_Mapper.class);
        job1.setReducerClass(MxN_Reducer.class);
        // Setting output types for the map phase
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Triple.class);
        // Setting output types for the reducer
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        if (!job1.waitForCompletion(true)) {
            System.exit(1); // Exit if first job fails
        }
        // Second job: Summarizing results
        Job job2 = Job.getInstance();
        job2.setJobName("Summarize Multiplication Results");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(MxN_Map.class);
        job2.setReducerClass(MxN_Red.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        if (!job2.waitForCompletion(true)) {
            System.exit(1); // Exit if second job fails
        }
        System.out.println("Matrix multiplication completed successfully.");
    }
}
