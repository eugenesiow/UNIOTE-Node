package uk.ac.soton.ldanalytics.uniote.node;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.zeromq.ZMQ;

import uk.ac.soton.ldanalytics.sparql2sql.model.RdfTableMapping;
import uk.ac.soton.ldanalytics.sparql2sql.model.SparqlOpVisitor;
import uk.ac.soton.ldanalytics.sparql2stream.parser.StreamQueryFactory;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class QueryServer {

	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);
		
		QueryTable queries = new QueryTable();
		
		String configFile = "config.json";
		
		Properties prop = new Properties();
		
		try {
			// load a properties file
			prop.load(new FileInputStream("config.properties"));
			String[] brokers = prop.getProperty("subscribe_brokers").split(";");
			
	        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
	        for(String broker:brokers) {
	        	subscriber.connect(broker);
	        }
	        
	        if(args.length>=1)
	        	configFile = args[0];
			
			Gson g = new Gson();
			JsonReader reader = new JsonReader(new FileReader(configFile));
			final StreamConfig config = g.fromJson(reader, StreamConfig.class);
			reader.close();
			
		//  Subscribe on datastreams registered
	        subscriber.subscribe(config.getUri().getBytes());
	        
	        RdfTableMapping mapping = new RdfTableMapping();
	    	mapping.loadMapping(config.getMapping());
			Map<String,String> streamCatalog = new HashMap<String,String>();
			streamCatalog.put(config.getName(), config.getUri());
			
			final NodeCepEngine engine = new NodeCepEngine("cep_engine",queries);
			engine.AddStream(config.getName(), config.getFormat());

			Executors.newSingleThreadExecutor().execute(new Runnable() {
			    public void run() {
			    	StreamReplayConfig replay = config.getReplay();
			    	engine.PlayFromCSV(config.getName(), replay.getSource(), replay.hasHeader(), replay.getTimeCol(), replay.getTimeFormat());
			    }
			});

	        //  wait for messages
	        while (!Thread.currentThread ().isInterrupted ()) {        	
	            String sub = subscriber.recvStr();
	            String qid = subscriber.recvStr();
	            String add = subscriber.recvStr();
	            String queryStr = subscriber.recvStr();
	            String queryHash = Long.toString(queryStr.hashCode());
//	            System.out.println(qid + ":" + add + ":" + queryStr + ":" + queryHash);
	            
	            Query query = StreamQueryFactory.create(queryStr);
	    		Op op = Algebra.compile(query);
//	    		System.out.println(op);
	    		
	    		SparqlOpVisitor v = new SparqlOpVisitor();
	    		v.useMapping(mapping);
	    		v.setNamedGraphs(query.getNamedGraphURIs());
	    		v.setStreamCatalog(streamCatalog);
	    		OpWalker.walk(op,v);
	    		
//	    		SQLFormatter formatter = new SQLFormatter();
//	    		System.out.println(formatter.format(v.getSQL()));
	    		
	    		queries.add(queryHash, add, qid);
	    		engine.AddQuery(v.getSQL(),queryHash);
	        }
	        
	        engine.shutdown();

	        subscriber.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		context.term();
        
	}

}
