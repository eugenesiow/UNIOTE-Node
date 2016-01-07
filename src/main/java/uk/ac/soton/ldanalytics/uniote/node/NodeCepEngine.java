package uk.ac.soton.ldanalytics.uniote.node;

import org.zeromq.ZMQ;

import uk.ac.soton.ldanalytics.sparql2stream.cep.CepEngine;

import com.espertech.esper.client.EPStatement;

public class NodeCepEngine extends CepEngine {
	
	private QueryTable queries = null;
	private ZMQ.Context context;

	public NodeCepEngine(String providerName, QueryTable queries) {
		super(providerName);
		context = ZMQ.context(2);
		this.queries = queries;
	}
	
	public void AddQuery(String queryStr,String queryHash) {
		EPStatement statement = epService.getEPAdministrator().createEPL(queryStr);
        statement.addListener(new NodeQueryListener(queryHash,queries,context));
	}
	
	public void shutdown() {
		context.term();
	}
}
