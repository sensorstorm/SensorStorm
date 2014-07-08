package nl.tno.timeseries.stormcomponents;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.EmitParticleInterface;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.MetaParticleHandler;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelManager implements Serializable {
	protected Logger logger = LoggerFactory.getLogger(ChannelManager.class);

	private static final long serialVersionUID = 3141072548366321818L;

	private String channelId;
	private Operation operation;
	private Batcher batcher;
	private List<MetaParticleHandler> metaParticleHandlers;
	private Class<? extends Operation> operationClass;
	private Class<? extends Batcher> batcherClass;
	private EmitParticleInterface emitParticleHandler;

	@SuppressWarnings("rawtypes")
	private Map stormConfig;

	public ChannelManager(String channelId, Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass, @SuppressWarnings("rawtypes") Map conf, EmitParticleInterface emitParticleHandler) {
		this.channelId = channelId;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.stormConfig = conf;
		this.emitParticleHandler = emitParticleHandler;
		
		metaParticleHandlers = new ArrayList<MetaParticleHandler>();
		System.out.println("Channel manager for cannel "+channelId+" created");
	}

	
	/**
	 * 
	 * @param particle
	 * @return returns a list with one MetaParticle or zero or more DataParticles or null in case of an error
	 */
	public List<Particle> processParticle(Particle particle) {
		if (particle == null)
			return null;
		
		// make sure this channel manager has an operation and optional metaParticleHandlers 
		if (operation == null) {
			try {
				createOperation(particle);
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("For channel "+channelId+": can not create the operation ("+operationClass.getName()+") msg="+e);
				return null;
			}
		}
		
		// parse particles
		List<Particle> result = new ArrayList<Particle>(); 
		if (particle instanceof MetaParticle) {
			handleMetaParticle((MetaParticle)particle);
			result.add(particle);
		} else if (particle instanceof DataParticle) {
			List<List<DataParticle>> batchedParticles = batcher.batch((DataParticle)particle);
			if (batchedParticles != null) {
				for (List<DataParticle> batchedParticle : batchedParticles) {
					List<DataParticle> outputDataParticles = operation.execute(batchedParticle);
					if (outputDataParticles != null) { 
						result.addAll(outputDataParticles);
					}
				}
			}
		} else {
			logger.warn("For channel "+channelId+": unknown particle type ("+particle.getClass().getName()+") to process");
			return null;
		}
		
		return result;
	}

	
	private void createOperation(Particle firstParticle) throws InstantiationException, IllegalAccessException {
		batcher = batcherClass.newInstance();
		batcher.init(firstParticle.getChannelId(), firstParticle.getSequenceNr(), stormConfig);
		
		operation = operationClass.newInstance();
		operation.init(firstParticle.getChannelId(), firstParticle.getSequenceNr(), stormConfig);
		
		createMetaParticleHandlers(operation);
	}

	
	private void createMetaParticleHandlers(Operation operation) throws InstantiationException, IllegalAccessException {
		OperationDeclaration operationDeclaration = operation.getClass().getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> mph :  operationDeclaration.metaParticleHandlers()) {
			MetaParticleHandler newInstance = mph.newInstance();
			newInstance.init(operation, emitParticleHandler);
			metaParticleHandlers.add(newInstance);
		}
	}

	
	private void handleMetaParticle(MetaParticle metaParticle) {
		for (MetaParticleHandler mph: metaParticleHandlers) {
			mph.handleMetaParticle(metaParticle);
		}
	}
	
	
	public static List<Class<? extends Particle>> getOutputParticles(Class<? extends Operation> operationClass) {
		List<Class<? extends Particle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass.getAnnotation(OperationDeclaration.class);
		for (Class<? extends DataParticle> cl : od.outputs()) {
			result.add(cl);
		}
		for (Class<? extends MetaParticleHandler> cl : od.metaParticleHandlers()) {
			MetaParticleHandlerDecleration mphd = cl.getAnnotation(MetaParticleHandlerDecleration.class);
			result.add(mphd.metaParticle());
		}
		return result;
	}

}
