package uk.ac.soton.ldanalytics.uniote.node;

import java.util.HashMap;
import java.util.Map;

public class QueryTable {
	private Map<String,Map<String,String>> table = new HashMap<String,Map<String,String>>();
	
	public void add(String hash,String address,String queryId) {
		Map<String,String> entry = table.get(hash);
		if(entry==null) {
			entry = new HashMap<String,String>();
		}
		entry.put(address, queryId);
		table.put(hash, entry);
	}
	
	public int size() {
		return table.size();
	}
	
	public Map<String,String> get(String hash) {
		return table.get(hash);
	}
}
