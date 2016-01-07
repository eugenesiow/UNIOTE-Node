package uk.ac.soton.ldanalytics.uniote.node;

import uk.ac.soton.ldanalytics.sparql2stream.cep.CepEngine;

import com.espertech.esper.client.EPStatement;

public class NodeCepEngine extends CepEngine {

	public NodeCepEngine(String providerName) {
		super(providerName);
	}
	
	public void AddQuery(String queryStr) {
		EPStatement statement = epService.getEPAdministrator().createEPL(queryStr);
		String queryHash = Long.toString(queryStr.hashCode());
        statement.addListener(new NodeQueryListener(queryHash));
	}
}
