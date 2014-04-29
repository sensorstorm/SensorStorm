package nl.tno.timeseries.operation;

import java.util.List;

import nl.tno.timeseries.model.Streamable;

public interface SingleInputOperation <Output extends Streamable> extends Operation<Output>{

	public List<Output> execute(Streamable streamable) throws Exception;
	
}
