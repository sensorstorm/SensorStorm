package nl.tno.timeseries.operation;

import java.util.List;

import nl.tno.timeseries.model.Streamable;

public interface BatchOperation<Output extends Streamable> extends Operation<Output> {

	public List<Output> execute(List<Streamable> input) throws Exception;
	
}
