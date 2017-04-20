package com.orienit.kalyan.hadoop.training.database;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class KalyanDBExportJob extends Configured implements Tool {

	public static void main(String... args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new KalyanDBExportJob(), args);
		System.out.println("Status: " + status);
	}

	@Override
	public int run(String[] args) throws Exception {
		String driverClassName = "com.mysql.jdbc.Driver";
		String connectionUrl = "jdbc:mysql://localhost:3306/kalyan_test_db?user=root&password=hadoop";
		DBConfiguration.configureDB(getConf(), driverClassName, connectionUrl);

		Job job = new Job(getConf(), "DB Export");
		job.setJarByClass(KalyanDBExportJob.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setMapOutputKeyClass(EmployeeWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EmployeeWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		DBOutputFormat.setOutput(job, "employee_export", EmployeeWritable.fields);

		// DistributedCache.addLocalFiles(getConf(), args[1]);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, EmployeeWritable, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] vals = value.toString().split(",");
			int id = Integer.parseInt(vals[0]);
			String name = vals[1];
			String dept = vals[2];
			double salary = Double.parseDouble(vals[3]);

			EmployeeWritable emp = new EmployeeWritable();
			emp.setId(id);
			emp.setName(name);
			emp.setDept(dept);
			emp.setSalary(salary);

			context.write(emp, NullWritable.get());
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
