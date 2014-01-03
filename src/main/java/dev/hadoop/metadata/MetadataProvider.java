package dev.hadoop.metadata;

import java.io.IOException;


public interface MetadataProvider {
	public static final String METADATA_FILENAME_BASE = "#_metadata";

	/**
	 * Generate metadata files 
	 * @return An array of metadata file URIs
	 * @throws IOException
	 */
	String[] generateMetadata() throws IOException;
	
}
