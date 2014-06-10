package nl.tno.timeseries.stormcomponents;

import java.util.List;
import java.util.Map;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationContext {
	protected Logger logger = LoggerFactory.getLogger(OperationContext.class);
	protected @SuppressWarnings("rawtypes")Map stormConfig;

	protected Operation operation = null;
	protected Class<? extends Operation> operationClass;
	
	public OperationContext(Class<? extends Operation> operationClass, @SuppressWarnings("rawtypes")Map conf) {
		this.operationClass = operationClass;
		this.stormConfig = conf;
	}
	
	public Operation getOperation() {
		if (operation == null) {
			try {
				operation = operationClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("Can not instantiate Operation class "+operationClass.getName());
				operation = null;
			}
		}
		return operation;
	}

	
	public List<Particle> processMetaParticle(MetaParticle metaParticle) {
		System.out.println("Meta particle received "+metaParticle);
		//@TODO timers and shutdown 
		return null;
	}
	
}
