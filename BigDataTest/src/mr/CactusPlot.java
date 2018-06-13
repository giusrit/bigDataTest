package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CactusPlot {

	/**
	 * 
	 * questa classe serve per comparare due valori in base al solver e al time
	 * 
	 */
	public static class MyComparable implements WritableComparable<MyComparable> {

		private String solver;
		private String time;

		public MyComparable() {
		}

		public MyComparable(String solver, String time) {
			super();
			this.solver = solver;
			this.time = time;
		}

		@Override
		public int compareTo(MyComparable o) {
			int cmp = solver.compareTo(o.solver);
			double a = Double.parseDouble(time);
			double b = Double.parseDouble(o.time);
			if (cmp == 0) {
				if (a > b)
					return 1;
				else if (a < b)
					return -1;
				else
					return 0;
			}
			return cmp;
		}

		@Override
		public String toString() {
			return String.format("%s\t%s\t", solver, time);
		}

		public String getSolver() {
			return solver;
		}

		public void setSolver(String solver) {
			this.solver = solver;
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			solver = WritableUtils.readString(arg0);
			time = WritableUtils.readString(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, solver);
			WritableUtils.writeString(arg0, time);
		}

	}

	/**
	 * 
	 * ordina le coppie 'solver','time' in modo crescente usando MyComparable
	 *
	 */
	public static class my_Comparator extends WritableComparator {
		public my_Comparator() {
			super(MyComparable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MyComparable a1 = (MyComparable) a;
			MyComparable b1 = (MyComparable) b;
			return -super.compare(a1, b1);
		}
	}

	/**
	 * 
	 * usato per raggruppare i valore in reducer
	 *
	 */
	public static class GroupComparator extends WritableComparator {

		protected GroupComparator() {
			super(MyComparable.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			MyComparable ac = (MyComparable) a;
			MyComparable bc = (MyComparable) b;
			if (ac.solver.equals(bc.solver))
				return 0;
			return 1;
		}

	}

	/**
	 * 
	 * Il mapper splitta l'input ad ogni tab, scarta le righe che non sono 'solved'
	 * e la riga di intestazione restituisce in context come key la coppia
	 * (solver,time)e come value time
	 *
	 */
	public static class Mapper_1 extends Mapper<LongWritable, Text, MyComparable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");

			if (!(row[0]).equals("Solver") && (row[14].equals("solved"))) {
				context.write(new MyComparable(row[0], row[11]), new Text(row[11]));
			}
		}
	}

	/**
	 * 
	 * splitta i valori per tab e li restituisce nel context,così avremo una riga
	 * per ogni solver seguita da tutti i suoi vaalori ordinati
	 *
	 */
	public static class Reducer_1 extends Reducer<MyComparable, Text, Text, Text> {

		public void reduce(MyComparable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String str = "";
			for (Text value : values) {
				str += value;
				str += "\t";
			}
			context.write(new Text(key.solver), new Text(str));

		}
	}

	// public static class Mapper_2 extends Mapper<LongWritable, Text, Text, Text> {
	//
	// @Override
	// protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
	// Text, Text>.Context context)
	// throws IOException, InterruptedException {
	//
	// String[] row = value.toString().split("\t");
	// context.write(new Text(row[0]), new Text(row[1]));
	// }
	// }
	//
	// public static class Reducer_2 extends Reducer<Text, Text, Text, Text> {
	//
	// public void reduce(Text key, Iterable<Text> values, Context context) throws
	// IOException, InterruptedException {
	//
	// }
	// }

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "catusPlotJob1");
		job.setJarByClass(CactusPlot.class);

		job.setMapperClass(Mapper_1.class);
		job.setReducerClass(Reducer_1.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setOutputKeyClass(MyComparable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// boolean next = job.waitForCompletion(true);
		//
		// if (next) {
		// Job job2 = Job.getInstance(conf, "catusPlotJob1");
		//
		// job2.setMapperClass(Mapper_2.class);
		// job2.setReducerClass(Reducer.class);
		//
		// job2.setOutputKeyClass(IntWritable.class);
		// job2.setOutputValueClass(Text.class);
		//
		// FileInputFormat.addInputPath(job2, new Path(args[1] + "/middle"));
		// FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
		//
		// job2.waitForCompletion(true);
		// }

		// System.exit(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}