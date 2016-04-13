package cn.javaman.etl.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;




/**
 * @author sunxy
 * @version 2016-1-6
 * @des 将ES args[0]中args[1]数据复制到到args[2]
 * 	ES_QUERY 为查询条件
 **/
public class HadoopEs  extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        args = new String[]{"172.20.0.194:9200,172.20.0.196:9200","index/type","javaman/yy"};
        ToolRunner.run(new HadoopEs(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
    	getConf().set(ConfigurationOptions.ES_NODES, args[0]);
//        getConf().set(ConfigurationOptions.ES_RESOURCE, args[1]);
	    getConf().set(ConfigurationOptions.ES_RESOURCE_READ, args[1]);
	    getConf().set(ConfigurationOptions.ES_RESOURCE_WRITE, args[2]);
	    getConf().set(ConfigurationOptions.ES_MAPPING_ID, "timestamp");
        getConf().set(ConfigurationOptions.ES_QUERY, "?q=sample:2");
        
        Job job = Job.getInstance(getConf());
        job.setJobName("ES Sample YY");
        job.setJarByClass(HadoopEs.class);

        job.setInputFormatClass(EsInputFormat.class);
        job.setMapperClass(EsMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LinkedMapWritable.class);
        
        job.setReducerClass(EsReducer.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LinkedMapWritable.class);

        boolean result = job.waitForCompletion(true) ;
        return result? 0 : 1;
    }

    public static class EsMapper extends Mapper<Text, LinkedMapWritable, Text, LinkedMapWritable> {

        @Override
        protected void map(Text key, LinkedMapWritable value, Context context) 
        		throws IOException, InterruptedException {
            context.write(key, value);
        }

    }

    public static class EsReducer extends Reducer<Text, LinkedMapWritable,  Text, LinkedMapWritable> {
        public void reduce(Text key, Iterable<LinkedMapWritable> values,Context context) 
        		throws IOException, InterruptedException {
            for (LinkedMapWritable value : values) {
        		context.write(key, value);
            }
        }
    }
}