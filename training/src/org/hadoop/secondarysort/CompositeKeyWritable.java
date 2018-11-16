package org.hadoop.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	
	private Text lname,fname;
	private IntWritable age;
	
	public CompositeKeyWritable() {
		lname = new Text();
		fname = new Text();
		age = new IntWritable();
	}
	
	
	
	public Text getLname() {
		return lname;
	}
	public void setLname(Text lname) {
		this.lname = lname;
	}
	public Text getFname() {
		return fname;
	}
	public void setFname(Text fname) {
		this.fname = fname;
	}



	public IntWritable getAge() {
		return age;
	}



	public void setAge(IntWritable age) {
		this.age = age;
	}



	@Override
	public int compareTo(CompositeKeyWritable o) {
		int value = age.compareTo(o.getAge());
		if( value == 0){
              value = fname.compareTo(o.getFname());
			
			if(value == 0){
				value = -1 * (lname.compareTo(o.lname)); 
		}
		
		}
		 
		return value;
	}

	@Override
	public String toString(){
		return (new StringBuilder().append(age).append(" ").append(fname).append(" ").append(lname).toString());
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.fname = new Text(in.readUTF());
		this.lname = new Text(in.readUTF());
		this.age = new IntWritable(in.readInt());
		
	}



	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(fname.toString());
		out.writeUTF(lname.toString());
		out.writeInt(age.get());
		
	}




}
