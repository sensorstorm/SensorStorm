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

/**
 * A channelManager manages for a specific channel the operation instance and
 * optional batcher and optional one or more metahandlers.
 * 
 * @author waaijbdvd
 * 
 */
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

	/**
	 * Creates a new ChannelManager for a specific channel with a batcher
	 * operation.
	 * 
	 * @param channelId
	 *            The id of the channel this channelManager works for.
	 * @param batcherClass
	 *            The class of the batcher to be used.
	 * @param batchOperationClass
	 *            The class of the batched operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 * @param emitParticleHandler
	 *            A reference to emit particles, to be used by
	 *            metaParticlehandlers.
	 */
	public ChannelManager(String channelId,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass,
			@SuppressWarnings("rawtypes") Map conf,
			EmitParticleInterface emitParticleHandler) {
		channelManager(channelId, batcherClass, batchOperationClass, conf,
				emitParticleHandler);
	}

	/**
	 * Creates a new ChannelManager for a specific channel with a single
	 * operation.
	 * 
	 * @param channelId
	 *            The id of the channel this channelManager works for.
	 * @param batchOperationClass
	 *            The class of the single operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 * @param emitParticleHandler
	 *            A reference to emit particles, to be used by
	 *            metaParticlehandlers.
	 */
	public ChannelManager(String channelId,
			Class<? extends SingleOperation> operationClass,
			@SuppressWarnings("rawtypes") Map conf,
			EmitParticleInterface emitParticleHandler) {
		channelManager(channelId, null, operationClass, conf,
				emitParticleHandler);
	}

	/**
	 * An internal class to create the channelManager
	 * 
	 * @param channelId
	 * @param batcherClass
	 * @param operationClass
	 * @param conf
	 * @param emitParticleHandler
	 */
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
		logger.debug("Channel manager for channel " + channelId + " created");
	}

	/**
	 * Process a particle (either MetaParticle or DataParticle)
	 * 
	 * @param particle
	 *            Particle to be processed. If the particle == null, null is
	 *            returned
	 * @return returns a list with one MetaParticle (to be sent further upto the
	 *         topology), zero or more DataParticles or null in case of an
	 *         error. These particles should be emitted by the bolt.
	 */
	public List<Particle> processParticle(Particle particle) {
		if (particle == null)
			return null;

		// make sure this channel manager has an operation and optional
		// metaParticleHandlers
		if (operation == null) { // this is the first particle, no operation has
									// been instantiated.
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

		if (particle instanceof MetaParticle) { // metaParticle
			handleMetaParticle((MetaParticle) particle);
			// add metaParticle to output list in order to be resent further in
			// the topology
			result.add(particle);

		} else if (particle instanceof DataParticle) { // dataParticle
			List<DataParticle> outputDataParticles = null;
			if (batcher != null) { // batch dataParticle and give it to
									// batcherOperation
				List<DataParticleBatch> batchedParticles = batcher
						.batch((DataParticle) particle);
				// are there one or more batches to be sent?
				if (batchedParticles != null) {
					for (DataParticleBatch batchedParticle : batchedParticles) {
						outputDataParticles = ((BatchOperation) operation)
								.execute(batchedParticle);
					}
				}
			} else { // single operation
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

	/**
	 * Create a new operation with Batcher and metaParticleHandlers
	 * 
	 * @param firstParticle
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void createOperation(Particle firstParticle)
			throws InstantiationException, IllegalAccessException {
		// create optional batcher
		if (batcherClass != null) {
			batcher = batcherClass.newInstance();
			batcher.init(firstParticle.getChannelId(),
					firstParticle.getTimestamp(), stormConfig);
		}

		// create new operation and initialize it
		operation = operationClass.newInstance();
		if (operationClass.isAssignableFrom(BatchOperation.class)) {
			((BatchOperation) operation).init(firstParticle.getChannelId(),
					firstParticle.getTimestamp(), stormConfig);
		} else if (operationClass.isAssignableFrom(SingleOperation.class)) {
			operation.init(firstParticle.getChannelId(),
					firstParticle.getTimestamp(), stormConfig);
		} else {
			logger.error("OperationClass of unknown type "
					+ operationClass.getName() + ", expected: "
					+ SingleOperation.class.getName() + " or "
					+ BatchOperation.class.getName());
		}

		createMetaParticleHandlers(operation);
	}

	/**
	 * Create all metaHandlers as specified in the annotations
	 * 
	 * @param operation
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void createMetaParticleHandlers(Operation operation)
			throws InstantiationException, IllegalAccessException {
		// walk through all specified metaHandlers in the annotation
		OperationDeclaration operationDeclaration = operation.getClass()
				.getAnnotation(OperationDeclaration.class);
		for (Class<? extends MetaParticleHandler> mph : operationDeclaration
				.metaParticleHandlers()) {
			MetaParticleHandler newInstance = mph.newInstance();
			newInstance.init(operation, emitParticleHandler);
			metaParticleHandlers.add(newInstance);
		}
	}

	/**
	 * pass this metaParticle to all metaHandlers.
	 * 
	 * @param metaParticle
	 */
	private void handleMetaParticle(MetaParticle metaParticle) {
		for (MetaParticleHandler mph : metaParticleHandlers) {
			MetaParticleHandlerDecleration mphd = mph.getClass().getAnnotation(
					MetaParticleHandlerDecleration.class);
			if (metaParticle.getClass().isAssignableFrom(mphd.metaParticle())) {
				mph.handleMetaParticle(metaParticle);
			}
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
