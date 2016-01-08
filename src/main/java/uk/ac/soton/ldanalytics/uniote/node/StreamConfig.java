package uk.ac.soton.ldanalytics.uniote.node;

public class StreamConfig {
	private String name;
	private String uri;
	private String mapping_src;
	private String format_src;
	private StreamReplayConfig replay;
	
	public String getUri() {
		return uri;
	}
	
	public String getName() {
		return name;
	}
	
	public String getMapping() {
		return mapping_src;
	}
	
	public String getFormat() {
		return format_src;
	}
	
	public StreamReplayConfig getReplay() {
		return replay;
	}
}
