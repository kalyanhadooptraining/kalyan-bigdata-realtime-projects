package com.orienit.kalyan.hadoop.training.database;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class KalyanDBImport extends Configured implements Tool {

	public static void main(String... args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new KalyanDBImport(), args);
		System.out.println("Status: " + status);
	}

	@Override
	public int run(String[] args) throws Exception {
		String driverClassName = "com.mysql.jdbc.Driver";
		String connectionUrl = "jdbc:mysql://localhost:3306/kalyan_test_db?user=root&password=hadoop";
		DBConfiguration.configureDB(getConf(), driverClassName, connectionUrl);

		Job job = new Job(getConf(), "DB Import");
		job.setJarByClass(KalyanDBImport.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(EmployeeWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EmployeeWritable.class);
		job.setOutputValueClass(NullWritable.class);

		DBInputFormat.setInput(job, EmployeeWritable.class, "select * from employee", "SELECT COUNT(id) FROM employee");

		// DistributedCache.addLocalFiles(getConf(), args[1]);

		Path outputPath = new Path(args[0]);

		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(getConf()).delete(outputPath, true);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, EmployeeWritable, EmployeeWritable, NullWritable> {
		@Override
		protected void map(LongWritable key, EmployeeWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

	public static class Reduce extends Reducer<EmployeeWritable, NullWritable, EmployeeWritable, NullWritable> {
		@Override
		public void reduce(EmployeeWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}
