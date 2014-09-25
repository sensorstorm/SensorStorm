package nl.tno.timeseries.channels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
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

	private String channelId; // The channel id this manager works for. Mostly
								// the channelId of the Particle, in case of a
								// ChannelGrouper the channelGroupId
	private Operation operation;
	private Batcher batcher;
	private List<MetaParticleHandler> metaParticleHandlers;
	private Class<? extends Operation> operationClass;
	private Class<? extends Batcher> batcherClass;
	private ZookeeperStormConfigurationAPI zookeeperStormConfiguration;
	private @SuppressWarnings("rawtypes")
	Map stormNativeConfig;

	/**
	 * Creates a new ChannelManager for a specific channel with a batcher
	 * operation.
	 * 
	 * @param channelId
	 *            The id of the channel this channelManager works for. In case
	 *            of a ChannelGrouperBolt infront of this bolt, the
	 *            channelGroupId will be used as set by the ChannelGrouper.
	 * @param batcherClass
	 *            The class of the batcher to be used.
	 * @param batchOperationClass
	 *            The class of the batched operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 */
	public ChannelManager(String channelId,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI stormConfiguration) {
		channelManager(channelId, batcherClass, batchOperationClass,
				stormNativeConfig, stormConfiguration);
	}

	/**
	 * Creates a new ChannelManager for a specific channel with a single
	 * operation.
	 * 
	 * @param channelId
	 *            The id of the channel this channelManager works for. In case
	 *            of a ChannelGrouperBolt infront of this bolt, the
	 *            channelGroupId will be used as set by the ChannelGrouper.
	 * @param batchOperationClass
	 *            The class of the single operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 */
	public ChannelManager(String channelId,
			Class<? extends SingleOperation> operationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI stormConfiguration) {
		channelManager(channelId, null, operationClass, stormNativeConfig,
				stormConfiguration);
	}

	/**
	 * An internal class to create the channelManager
	 * 
	 * @param channelId
	 * @param batcherClass
	 * @param operationClass
	 * @param conf
	 */
	private void channelManager(String channelId,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI stormConfiguration) {
		this.channelId = channelId;
		this.operationClass = operationClass;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.zookeeperStormConfiguration = stormConfiguration;

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
			List<Particle> outputParticles = handleMetaParticle((MetaParticle) particle);
			// add metaParticle to output list in order to be resent further in
			// the topology
			result.add(particle);
			// add optional output particles
			if (outputParticles != null) {
				result.addAll(outputParticles);
			}
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
			batcher.init(channelId, firstParticle.getTimestamp(),
					stormNativeConfig, zookeeperStormConfiguration);
		}

		// create new operation and initialize it
		operation = operationClass.newInstance();
		// is it a BatchOperation?
		if (BatchOperation.class.isInstance(operation)) {
			((BatchOperation) operation).init(channelId,
					firstParticle.getTimestamp(), stormNativeConfig,
					zookeeperStormConfiguration);
		} // or is it a SingleOperation?
		else if (SingleOperation.class.isInstance(operation)) {
			operation.init(channelId, firstParticle.getTimestamp(),
					stormNativeConfig, zookeeperStormConfiguration);
		} // unknown operation type
		else {
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
			newInstance.init(operation);
			metaParticleHandlers.add(newInstance);
		}
	}

	/**
	 * pass this metaParticle to all metaHandlers.
	 * 
	 * @param metaParticle
	 */
	private List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		List<Particle> result = null;
		for (MetaParticleHandler mph : metaParticleHandlers) {
			MetaParticleHandlerDecleration mphd = mph.getClass().getAnnotation(
					MetaParticleHandlerDecleration.class);
			if (metaParticle.getClass().isAssignableFrom(mphd.metaParticle())) {
				result = mph.handleMetaParticle(metaParticle);
			}
		}
		return result;
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
