package uk.ac.soton.ldanalytics.uniote.node;

import java.util.Map;
import java.util.Map.Entry;

import org.zeromq.ZMQ;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class NodeQueryListener implements UpdateListener {
	private String queryName;
	private QueryTable queries;
	private ZMQ.Context context;
	
	public NodeQueryListener(String queryName, QueryTable queries, ZMQ.Context context) {
		this.context = context;
		this.queryName = queryName;
		this.queries = queries;
	}

	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if(newEvents.length>0) {
			JsonObject message = new JsonObject();
			message.addProperty("queryName", queryName);
			for(int i=0;i<newEvents.length;i++) {
				for(Object map:((Map<?, ?>)newEvents[i].getUnderlying()).entrySet()) {
					Entry<?, ?> entry = ((Entry<?, ?>)map);
//					System.out.println(entry.getKey()+":"+entry.getValue());
					String key = entry.getKey().toString();
					String val = entry.getValue().toString();
					if(message.has(key)) {
						JsonElement jVal = message.get(key);
						JsonArray newVal = new JsonArray();
						if(jVal.isJsonArray()) {
							newVal = jVal.getAsJsonArray();
						} else if(jVal.isJsonPrimitive()) {
							newVal.add(jVal.getAsString());
						}
						newVal.add(val);
						message.add(key, newVal);
					} else {
						message.addProperty(key,val);
					}
				}
			}
			
			Map<String,String> addList = queries.get(queryName);
			if(addList!=null) {
				for(Entry<String,String> addresses:addList.entrySet()) {
					ZMQ.Socket sender = context.socket(ZMQ.PUSH);
					sender.connect("tcp://"+addresses.getKey()+":5700");

					
//					sender.sendMore(queryName);
					sender.sendMore(addresses.getValue());
					sender.send(message.toString());
					
					System.out.println(message.toString());
					
					sender.close();
				}
			}
		}
	}

}
