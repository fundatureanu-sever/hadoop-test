package dev.hadoop;

import java.io.IOException;
import java.net.URI;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dev.hadoop.metadata.JDBCMetadataProvider;
import dev.hadoop.metadata.MetadataProvider;

public class ReportDriver extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		/*int numberOfNodes = 1;
		String inputPath = "input";
		String outputPath = "output";*/
		
		try {
			if (args.length != 3){
				System.err.println("Unexpected number of arguments");
				System.out.println("Usage: dev.hadoop.ReportDriver <N_slave_nodes> <input_path> <output_path>");
				return;
			}
			
			int exitCode = ToolRunner.run(new ReportDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		int numberOfNodes = Integer.parseInt(args[0]);
		
		Configuration conf = getConf();
		
		//TODO optimize parameters here
		
		try {
			Job j;
			j = new Job(conf);
			j.setJobName("ReportByUserID");
			
			j.setMapperClass(ReportByUserIdMapper.class);
			j.setReducerClass(ReportByUserIdReducer.class);
			
			j.setMapOutputKeyClass(IntWritable.class);
			j.setMapOutputValueClass(IntermediateUserInfo.class);
			
			j.setOutputKeyClass(NullWritable.class);
			j.setOutputValueClass(Text.class);
			
			j.setInputFormatClass(TextInputFormat.class);
			j.setOutputFormatClass(TextOutputFormat.class);
			
			j.setNumReduceTasks((int)(numberOfNodes*1.75));
			
			//TODO setup partitioner
			
			TextInputFormat.setInputPaths(j, new Path(args[1]));
			TextOutputFormat.setOutputPath(j, new Path(args[2]));
			
			MetadataProvider metadataProvider = new JDBCMetadataProvider();
			String metadataFileURI = metadataProvider.generateMetadata();
			DistributedCache.addCacheFile(new URI(metadataFileURI), conf);
			DistributedCache.createSymlink(conf);
			
			j.waitForCompletion(true);
			
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return -1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}

	
}
