package dev.hadoop.v2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dev.hadoop.constants.Constants;
import dev.hadoop.metadata.ExtendedJDBCMetadataProvider;
import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.intermediate.DataByCatIdAndQuarter;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

public class ReportDriver extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) {
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
		
		MetadataProvider metadataProvider = new ExtendedJDBCMetadataProvider();
		String[] metadataFileURIs = metadataProvider.generateMetadata();
		
		System.out.println("Starting first job ..");
		
		String tempDirectory = "tempDir";
		Job j1 = createFirstJob(numberOfNodes, args[1], tempDirectory, metadataFileURIs);
		j1.waitForCompletion(true);
		
		System.out.println("First job finished");
		System.out.println("Starting second job ..");
		
		Job j2 = createSecondJob(numberOfNodes, tempDirectory, args[2], metadataFileURIs);
		j2.waitForCompletion(true);
		
		System.out.println("Second job finished");
		
		return 0;
	}

	private Job createFirstJob(int numberOfNodes, String inputPath, String outputPath, String []metadataFileURIs) throws IOException, URISyntaxException{
		Configuration conf = new Configuration(getConf());
		
		conf.setBoolean("mapred.compress.map.output", true);
		
		//estimating the size of an input line in text
		int inputRecordSize = 10+5+5+5+5+6+10;//date+order_id+prod_id+user_id+actMngr_id+quantity+N_tabs
		
		//estimating the size of an map output record in bytes 
		int mapOutputRecordSize = (4+4)+(4+4+1);//key(UserId+CategoryId)+value(ProductId+Quantity+Quarter)
				
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(Constants.INPUT_SPLIT_SIZE,
													inputRecordSize, mapOutputRecordSize, 1);
		configureShuffle(conf, shuffleOptimizer);
		
		FileSystem fs = FileSystem.get(conf);
		for (int i = 0; i < metadataFileURIs.length; i++) {
			System.out.println(metadataFileURIs[i]);
			fs.copyFromLocalFile(new Path(metadataFileURIs[i].split("#")[0]), new Path(metadataFileURIs[i]));
			DistributedCache.addCacheFile(new URI(metadataFileURIs[i]), conf);
		}		
		DistributedCache.createSymlink(conf);
		
		Job j = new Job(conf);
		j.setJobName("ReportByUserIDCategoryQuarter");
		
		j.setJarByClass(ReportDriver.class);
		j.setMapperClass(ReportByUserCategQuarterMapper.class);
		j.setReducerClass(ReportByUserCategQuarterReducer.class);
		
		j.setMapOutputKeyClass(UserCategoryId.class);
		j.setMapOutputValueClass(ProductIdQuantityQuarter.class);
		
		j.setOutputKeyClass(IntWritable.class);
		j.setOutputValueClass(DataByCatIdAndQuarter.class);
		
		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		TextInputFormat.setInputPaths(j, new Path(inputPath));
		SequenceFileOutputFormat.setOutputPath(j, new Path(outputPath));
		
		j.setNumReduceTasks((int)(numberOfNodes*1.75));
		
		
		return j;
	}
	
	private Job createSecondJob(int numberOfNodes, String inputPathString, String outputPath, String[] metadataFileURIs) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration(getConf());
		
		conf.setBoolean("mapred.compress.map.output", true);
		
		//estimating the size of an input line in text
		int inputRecordSize = (4+4+1)+(4+8);//key(userId+categId+Quarter)+value(quantity+revenue)
				
		//estimating the size of an map output record in bytes 
		int mapOutputRecordSize = 4+(4+1+4+8);//key(userId+categId+Quarter)+value(quantity+revenue)
					
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(Constants.INPUT_SPLIT_SIZE,
													inputRecordSize, mapOutputRecordSize, 1);
		configureShuffle(conf, shuffleOptimizer);
		
		//FileSystem fs = FileSystem.get(conf);
		for (int i = 0; i < metadataFileURIs.length; i++) {
			System.out.println(metadataFileURIs[i]);
			//fs.copyFromLocalFile(new Path(metadataFileURIs[i].split("#")[0]), new Path(metadataFileURIs[i]));
			DistributedCache.addCacheFile(new URI(metadataFileURIs[i]), conf);
		}		
		DistributedCache.createSymlink(conf);
		
		Job j = new Job(conf);
		j.setJobName("ReportByUserID");
		
		j.setJarByClass(ReportDriver.class);
		j.setMapperClass(Mapper.class);
		j.setReducerClass(ReportByUserIdReducer.class);
		
		j.setMapOutputKeyClass(IntWritable.class);
		j.setMapOutputValueClass(DataByCatIdAndQuarter.class);
		
		j.setOutputKeyClass(NullWritable.class);
		j.setOutputValueClass(Text.class);
		
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputPath = new Path(inputPathString);
		SequenceFileInputFormat.setInputPaths(j, inputPath);
		TextOutputFormat.setOutputPath(j, new Path(outputPath));
		
		configurePartitioner(conf, j, inputPath);	
		
		j.setNumReduceTasks((int)(numberOfNodes*1.75));
		
		return j;
	}

	private void configurePartitioner(Configuration conf, Job j, Path inputPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		j.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<IntWritable, DataByCatIdAndQuarter> sampler = new InputSampler.RandomSampler<IntWritable, DataByCatIdAndQuarter>(0.1, 1000, 20);
		
		Path partitionFile = new Path(inputPath, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(j, sampler);
		
		URI partitionURI = new URI(partitionFile.toString()+"#_partitions");
		DistributedCache.addCacheFile(partitionURI, conf);
	}

	private void configureShuffle(Configuration conf, ShuffleStageOptimizer shuffleOptimizer) {
		conf.setInt("io.sort.mb", shuffleOptimizer.getIoSortMB());
		conf.setFloat("io.sort.spill.percent", shuffleOptimizer.getIoSortSpillThreshold());
		conf.setFloat("io.sort.record.percent", shuffleOptimizer.getIoSortRecordPercent());
		conf.setInt("io.sort.factor", shuffleOptimizer.getIoSortFactor());
		conf.setFloat("mapred.job.reduce.input.buffer.percent", ShuffleStageOptimizer.REDUCE_BUFFER_PERCENT);
	}
	
}
