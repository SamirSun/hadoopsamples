package cn.javaman.etl.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author sunxy
 * @version 2016-4-12
 * @des 将数据库中的数据复制到其他的表中.
 **/
public class HadoopMysql extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		DBConfiguration.configureDB(getConf(),"com.mysql.jdbc.Driver",
                args[0],args[1],args[2]);
		Job job = Job.getInstance(getConf());
		job.setJobName("MR - Mysql");
        job.setJarByClass(HadoopMysql.class);
        job.setMapperClass(MysqlMapper.class);
        job.setReducerClass(MysqlReducer.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MapWritable.class);
        
        //map输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        /**两种获取数据的方法*/
        DBInputFormat.setInput(job, Table.class, 
        		"SELECT id,name FROM t", 
        		"SELECT count(*) FROM t");
        /*String[] fields = {"id","name"};
        DBInputFormat.setInput(job, Table.class,
                "t", "id > 0", "id", fields); */
        DBOutputFormat.setOutput(job, "t2", "id","name");
		
        return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class MysqlMapper extends 
				Mapper<LongWritable, Table, LongWritable, MapWritable> {
        @Override
        protected void map(LongWritable key, Table value, Context context) 
        		throws IOException, InterruptedException {
        	MapWritable mw = new MapWritable();
        	mw.put(new Text("id"), new IntWritable(value.id));
        	mw.put(new Text("name"), new Text(value.name));
            context.write(key, mw);
        }
    }

	public static class MysqlReducer extends Reducer<LongWritable, MapWritable,  Table,LongWritable> {
        public void reduce(LongWritable key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {
            for (MapWritable value : values) {
            	Table tab = new Table();
            	int id = Integer.parseInt(value.get(new Text("id")).toString());
            	String name = value.get(new Text("name")).toString();
            	System.out.println("___reduce id/name:"+id+"/"+name);
            	tab.id = id;
            	tab.name = name;
            	context.write(tab, key);
            }
        }
    }

	public static void main(String[] args) throws Exception {
		//args内容 "jdbc:mysql://ip/database","account","password"
		Configuration conf = new Configuration();
        ToolRunner.run(conf, new HadoopMysql() , args);
	}
	public static class Table implements Writable, DBWritable {
		int id;
		String name;
		public Table() {
		}
		public void readFields(DataInput in) throws IOException {
		}
		@Override
		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
			System.out.println("===write===stmt:"+stmt);
		}
		@Override
		public void readFields(ResultSet result) throws SQLException {
			this.id = result.getInt("id");
			this.name = result.getString("name");
		}
		@Override
		public void write(DataOutput out) throws IOException {
		}
	}

}
