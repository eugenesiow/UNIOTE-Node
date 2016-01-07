package uk.ac.soton.ldanalytics.uniote.node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.zeromq.ZMQ;

import uk.ac.soton.ldanalytics.sparql2sql.model.RdfTableMapping;
import uk.ac.soton.ldanalytics.sparql2sql.model.SparqlOpVisitor;
import uk.ac.soton.ldanalytics.sparql2stream.parser.StreamQueryFactory;


public class QueryServer {

	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);
		
		QueryTable queries = new QueryTable();

        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
        subscriber.connect("tcp://localhost:5600");

        //  Subscribe on datastreams registered
        subscriber.subscribe("http://www.cwi.nl/SRBench/observations".getBytes());
        
        RdfTableMapping mapping = new RdfTableMapping();
    	mapping.loadMapping("/Users/eugene/Downloads/knoesis_observations_ike_map_meta/HP001.nt");
		Map<String,String> streamCatalog = new HashMap<String,String>();
		streamCatalog.put("_HP001", "http://www.cwi.nl/SRBench/observations");
		
		final NodeCepEngine engine = new NodeCepEngine("cep_engine",queries);
		engine.AddStream("_HP001", "/Users/eugene/Documents/workspace/sparql2stream/format/_HP001.format");
		
		Executors.newSingleThreadExecutor().execute(new Runnable() {
		    public void run() {
		    	engine.PlayFromCSV("_HP001", "/Users/eugene/Documents/workspace/sparql2stream/samples/_HP001.csv", true, 0, "yyyy-MM-dd hh:mm:ss");
		    }
		});

        //  wait for messages
        while (!Thread.currentThread ().isInterrupted ()) {        	
            String sub = subscriber.recvStr();
            String qid = subscriber.recvStr();
            String add = subscriber.recvStr();
            String queryStr = subscriber.recvStr();
            String queryHash = Long.toString(queryStr.hashCode());
//            System.out.println(qid + ":" + add + ":" + queryStr + ":" + queryHash);
            
            Query query = StreamQueryFactory.create(queryStr);
    		Op op = Algebra.compile(query);
//    		System.out.println(op);
    		
    		SparqlOpVisitor v = new SparqlOpVisitor();
    		v.useMapping(mapping);
    		v.setNamedGraphs(query.getNamedGraphURIs());
    		v.setStreamCatalog(streamCatalog);
    		OpWalker.walk(op,v);
    		
//    		SQLFormatter formatter = new SQLFormatter();
//    		System.out.println(formatter.format(v.getSQL()));
    		
    		queries.add(queryHash, add, qid);
    		engine.AddQuery(v.getSQL(),queryHash);
        }
        
        engine.shutdown();

        subscriber.close();
        context.term();
	}

}
