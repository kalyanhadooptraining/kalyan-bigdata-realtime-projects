package com.orienit.kalyan.hadoop.training.database;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class EmployeeWritable implements WritableComparable<EmployeeWritable>, DBWritable {
	public static String[] fields = { "id", "name", "dept", "salary" };

	int id;
	String name;
	String dept;
	double salary;

	@Override
	public String toString() {
		return id + "," + name + "," + dept + "," + salary;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dept == null) ? 0 : dept.hashCode());
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		long temp;
		temp = Double.doubleToLongBits(salary);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EmployeeWritable other = (EmployeeWritable) obj;
		if (dept == null) {
			if (other.dept != null)
				return false;
		} else if (!dept.equals(other.dept))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (Double.doubleToLongBits(salary) != Double.doubleToLongBits(other.salary))
			return false;
		return true;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		name = in.readUTF();
		dept = in.readUTF();
		salary = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
		out.writeUTF(dept);
		out.writeDouble(salary);
	}

	@Override
	public int compareTo(EmployeeWritable key) {
		return CompareToBuilder.reflectionCompare(this, key);
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int idx = 1;
		statement.setInt(idx++, getId());
		statement.setString(idx++, getName());
		statement.setString(idx++, getDept());
		statement.setDouble(idx++, getSalary());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		int idx = 1;
		setId(resultSet.getInt(idx++));
		setName(resultSet.getString(idx++));
		setDept(resultSet.getString(idx++));
		setSalary(resultSet.getDouble(idx++));
	}
}
