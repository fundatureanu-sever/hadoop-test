package dev.hadoop.v2;

public class ShuffleStageOptimizer {
	
	private static final int META_BUFFER_UNIT_SIZE = 16;
	public static final float REDUCE_BUFFER_PERCENT = 0.5f;

	private long inputSplitSizeBytes;
	
	private long inputRecordSize;
	
	private long mapOutputRecordsSize;
	
	private long totalIoSortSizeBytes;
	
	private long ioSortSerializationBufferSize;
	
	private long ioSortMetaBufferSize;
	private int totalIoSortSizeMB;
	
	private int inToOutCardinality;//number of output records corresponding to 1 input record
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
								long mapOutputRecordSize, int cardinality) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.inToOutCardinality = cardinality;
		computeIoSortBufferSize();
	}
	
	final private void computeIoSortBufferSize(){
		long inputRecordsPerSplit = inputSplitSizeBytes/inputRecordSize+1;
		ioSortSerializationBufferSize = inputRecordsPerSplit*mapOutputRecordsSize;
		ioSortMetaBufferSize = inputRecordsPerSplit*META_BUFFER_UNIT_SIZE*inToOutCardinality;
		totalIoSortSizeBytes = ioSortMetaBufferSize+ioSortSerializationBufferSize;
		totalIoSortSizeMB = (int)(((double)totalIoSortSizeBytes)/1024/1024);
	}

	public int getIoSortMB() {
		return totalIoSortSizeMB;
	}
	
	public float getIoSortSpillThreshold(){
		return 0.95f;//the estimate is accurate enough so we are confident we are not going to spill
	}
	
	public float getIoSortRecordPercent(){
		return (float)ioSortMetaBufferSize/(float)totalIoSortSizeBytes;
	}
	
	public int getIoSortFactor(){
		return totalIoSortSizeMB/10+1;
	}
	
	public String toString(){
		String ret = "totalIoSortSizeMB "+totalIoSortSizeMB;
		return ret;
	}
}
