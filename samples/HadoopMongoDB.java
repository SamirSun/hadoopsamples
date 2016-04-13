package cn.javaman.etl.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;


/**
 * @author sunxy
 * @version 2015-9-8
 * @des MongoDB中数据复制
 **/
public class HadoopMongoDB {

	public static class MongoMap extends Mapper<Object, BSONObject, Text, BSONWritable> {
		public void map(Object key, BSONObject value, Context context) 
				throws IOException, InterruptedException {
			context.write(new Text(key.toString()),new BSONWritable(value));
		}
	}
	
	public static class MongoReducer extends Reducer<Text, BSONWritable,Text, BSONWritable> {
		
		public void reduce(Text key, Iterable<BSONWritable> values,Context context) 
				throws IOException, InterruptedException {
			for(BSONWritable value: values) {
			    context.write(key, value);
			 }
		 }
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{"mongodb://172.20.0.227:27017/cache.recom_result"
				,"mongodb://172.20.0.227:27017/cache.recom_result2"};
		Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,args[0]);
		MongoConfigUtil.setOutputURI(conf, args[1]);
		BasicDBObject query = new BasicDBObject();
		//query.put("status", "open");
		MongoConfigUtil.setQuery(conf, query);
		MongoConfigUtil.setCreateInputSplits(conf, false);
		final Job job = Job.getInstance(conf, "Hadoop Mongo");
		job.setJarByClass(HadoopMongoDB.class);
		job.setMapperClass(MongoMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);
		job.setCombinerClass(MongoReducer.class);
		job.setReducerClass(MongoReducer.class);
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}