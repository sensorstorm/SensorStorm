package nl.tno.timeseries.interfaces;

import backtype.storm.tuple.Tuple;

public interface FaultTolerant {

	public void ack(Tuple tuple);

	public void fail(Tuple tuple);
}
