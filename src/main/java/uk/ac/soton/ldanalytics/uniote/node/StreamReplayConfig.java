package uk.ac.soton.ldanalytics.uniote.node;

public class StreamReplayConfig {
	private String src;
	private int time_col;
	private String time_format;
	private Boolean header;
	
	public String getSource() {
		return src;
	}
	
	public int getTimeCol() {
		return time_col;
	}
	
	public String getTimeFormat() {
		return time_format;
	}
	
	public Boolean hasHeader() {
		return header;
	}
}
