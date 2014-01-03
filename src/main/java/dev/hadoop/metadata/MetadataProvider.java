package dev.hadoop.metadata;

import java.io.IOException;


public interface MetadataProvider {
	public static final String METADATA_FILE_NAME = "#_metadata";

	String generateMetadata() throws IOException;
	
}
