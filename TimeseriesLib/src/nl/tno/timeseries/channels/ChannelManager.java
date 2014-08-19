package nl.tno.timeseries.channels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.particles.EmitParticleInterface;
import nl.tno.timeseries.particles.MetaParticleHandler;

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

	public ChannelManager(String channelId,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass,
			@SuppressWarnings("rawtypes") Map conf,
			EmitParticleInterface emitParticleHandler) {
		channelManager(channelId, batcherClass, batchOperationClass, conf,
				emitParticleHandler);
	}

	public ChannelManager(String channelId,
			Class<? extends SingleOperation> operationClass,
			@SuppressWarnings("rawtypes") Map conf,
			EmitParticleInterface emitParticleHandler) {
		channelManager(channelId, null, operationClass, conf,
				emitParticleHandler);
	}

	private void channelManager(String channelId,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass,
			@SuppressWarnings("rawtypes") Map conf,
			EmitParticleInterface emitParticleHandler) {
		this.channelId = channelId;
		this.operationClass = operationClass;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.stormConfig = conf;
		this.emitParticleHandler = emitParticleHandler;

		metaParticleHandlers = new ArrayList<MetaParticleHandler>();
		System.out.println("Channel manager for channel " + channelId
				+ " created");
	}

	/**
	 * Process a particle (either MetaParticle or DataParticle)
	 * 
	 * @param particle
	 *            Particle to be processed
	 * @return returns a list with one MetaParticle, zero or more DataParticles
	 *         or null in case of an error. These particles should be emitted by
	 *         the bolt.
	 */
	public List<Particle> processParticle(Particle particle) {
		if (particle == null)
			return null;

		// make sure this channel manager has an operation and optional
		// metaParticleHandlers
		if (operation == null) {
			try {
				createOperation(particle);
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("For channel " + channelId
						+ ": can not create the operation ("
						+ operationClass.getName() + ") msg=" + e);
				return null;
			}
		}

		// parse particles
		List<Particle> result = new ArrayList<Particle>();
		if (particle instanceof MetaParticle) {
			handleMetaParticle((MetaParticle) particle);
			result.add(particle);
		} else if (particle instanceof DataParticle) {
			List<DataParticle> outputDataParticles = null;
			if (batcher != null) {
				List<DataParticleBatch> batchedParticles = batcher
						.batch((DataParticle) particle);
				if (batchedParticles != null) {
					for (DataParticleBatch batchedParticle : batchedParticles) {
						outputDataParticles = ((BatchOperation) operation)
								.execute(batchedParticle);
					}
				}
			} else {
				outputDataParticles = ((SingleOperation) operation)
						.execute((DataParticle) particle);
			}

			if (outputDataParticles != null) {
				result.addAll(outputDataParticles);
			}
		} else {
			logger.warn("For channel " + channelId
					+ ": unknown particle type ("
					+ particle.getClass().getName() + ") to process");
			return null;
		}

		return result;
	}

	private void createOperation(Particle firstParticle)
			throws InstantiationException, IllegalAccessException {
		batcher = batcherClass.newInstance();
		batcher.init(firstParticle.getChannelId(),
				firstParticle.getTimestamp(), stormConfig);

		operation = operationClass.newInstance();
		if (operationClass.isAssignableFrom(BatchOperation.class)) {
			((BatchOperation) operation).init(firstParticle.getChannelId(),
					firstParticle.getTimestamp(), stormConfig);
		} else if (operationClass.isAssignableFrom(Operation.class)) {
			operation.init(firstParticle.getChannelId(),
					firstParticle.getTimestamp(), stormConfig);
		}

		createMetaParticleHandlers(operation);
	}

	private void createMetaParticleHandlers(Operation operation)
			throws InstantiationException, IllegalAccessException {
		OperationDeclaration operationDeclaration = operation.getClass()
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> mph : operationDeclaration
				.metaParticleHandlers()) {
			MetaParticleHandler newInstance = mph.newInstance();
			newInstance.init(operation, emitParticleHandler);
			metaParticleHandlers.add(newInstance);
		}
	}

	private void handleMetaParticle(MetaParticle metaParticle) {
		for (MetaParticleHandler mph : metaParticleHandlers) {
			mph.handleMetaParticle(metaParticle);
		}
	}

	/**
	 * Given an Operation class, figure out all the types of particles (both
	 * DataParticles and MetaParticles) produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of Particle classes
	 */
	public static List<Class<? extends Particle>> getOutputParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends Particle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends DataParticle> cl : od.outputs()) {
			result.add(cl);
		}
		for (Class<? extends MetaParticleHandler> cl : od
				.metaParticleHandlers()) {
			MetaParticleHandlerDecleration mphd = cl
					.getAnnotation(MetaParticleHandlerDecleration.class);
			result.add(mphd.metaParticle());
		}
		return result;
	}

	/**
	 * Given an Operation class, figure out all the types of DataParticles
	 * produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of DataParticle classes
	 */
	public static List<Class<? extends DataParticle>> getOutputDataParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends DataParticle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends DataParticle> cl : od.outputs()) {
			result.add(cl);
		}
		return result;
	}

	/**
	 * Given an Operation class, figure out all the types of MetaParticles
	 * produced by this Operation
	 * 
	 * @param operationClass
	 *            Class of the Operation
	 * @return List of MetaParticle classes
	 */
	public static List<Class<? extends MetaParticle>> getOutputMetaParticles(
			Class<? extends Operation> operationClass) {
		List<Class<? extends MetaParticle>> result = new ArrayList<>();
		OperationDeclaration od = operationClass
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> cl : od
				.metaParticleHandlers()) {
			MetaParticleHandlerDecleration mphd = cl
					.getAnnotation(MetaParticleHandlerDecleration.class);
			result.add(mphd.metaParticle());
		}
		return result;
	}

}
