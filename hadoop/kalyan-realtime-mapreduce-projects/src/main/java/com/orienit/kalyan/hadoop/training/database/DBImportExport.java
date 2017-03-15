package com.orienit.kalyan.hadoop.training.database;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class DBImportExport extends Configured implements Tool {

	public static void main(String... args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new DBImportExport(), args);
		System.out.println("Status: " + status);
	}

	@Override
	public int run(String[] args) throws Exception {

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/kalyan_test_db"
				+ "?user=root&password=hadoop");

		Job job = new Job(getConf());
		job.setJarByClass(DBImportExport.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setMapOutputKeyClass(EmployeeWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EmployeeWritable.class);
		job.setOutputValueClass(NullWritable.class);

		DBInputFormat.setInput(job, EmployeeWritable.class, "select * from employee", "SELECT COUNT(id) FROM employee");

		DBOutputFormat.setOutput(job, "employee_export", EmployeeWritable.fields);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, EmployeeWritable, EmployeeWritable, NullWritable> {
		@Override
		protected void map(LongWritable key, EmployeeWritable value, Context context) throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

	public static class Reduce extends Reducer<EmployeeWritable, NullWritable, EmployeeWritable, NullWritable> {
		public void reduce(EmployeeWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}
