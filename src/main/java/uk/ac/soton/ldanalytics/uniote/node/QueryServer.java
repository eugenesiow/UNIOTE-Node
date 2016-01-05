package uk.ac.soton.ldanalytics.uniote.node;

import java.util.StringTokenizer;

import org.zeromq.ZMQ;

public class QueryServer {

	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
        subscriber.connect("tcp://localhost:5600");

        //  Subscribe on everything
        subscriber.subscribe("".getBytes());

        //  Process 100 updates
        int update_nbr;
        long total_temp = 0;
        for (update_nbr = 0; update_nbr < 100; update_nbr++) {
            //  Use trim to remove the tailing '0' character
            String string = subscriber.recvStr(0).trim();
            System.out.println(string);
        }

        subscriber.close();
        context.term();
	}

}
