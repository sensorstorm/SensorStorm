package nl.tno.timeseries.operations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.batchers.Batcher;
import nl.tno.timeseries.batchers.BatcherException;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.DataParticleBatch;
import nl.tno.timeseries.particles.MetaParticle;
import nl.tno.timeseries.particles.MetaParticleHandler;
import nl.tno.timeseries.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OperationManager manages for a specific fieldGrouper the operation
 * instance and optional batcher and optional one or more metahandlers.
 * 
 * @author waaijbdvd
 * 
 */
public class OperationManager implements Serializable {
	protected Logger logger = LoggerFactory.getLogger(OperationManager.class);

	private static final long serialVersionUID = 3141072548366321818L;

	private String fieldGrouperValue; // The fieldGroupValue this manager works
										// for.
	private Operation operation;
	private Batcher batcher;
	private List<MetaParticleHandler> metaParticleHandlers;
	private Class<? extends Operation> operationClass;
	private Class<? extends Batcher> batcherClass;
	private ExternalStormConfiguration zookeeperStormConfiguration;
	@SuppressWarnings("rawtypes")
	private Map stormNativeConfig;

	/**
	 * Creates a new OperationManager for a specific fieldGrouper with a batcher
	 * and operation.
	 * 
	 * @param fieldGroupValue
	 *            The fieldGroupValue this operationManager works for. An
	 *            operation will be created when a particle which matches the
	 *            fieldGroupValue arrives for the first time. If the
	 *            fieldGroupValue is null, only one instance of the operation
	 *            will be created directly within this constructor.
	 * @param batcherClass
	 *            The class of the batcher to be used.
	 * @param batchOperationClass
	 *            The class of the batched operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public OperationManager(String fieldGroupValue,
			Class<? extends Batcher> batcherClass,
			Class<? extends BatchOperation> batchOperationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration)
			throws InstantiationException, IllegalAccessException {
		operationManager(fieldGroupValue, batcherClass, batchOperationClass,
				stormNativeConfig, stormConfiguration);
	}

	/**
	 * Creates a new OperationManager for a specific channel with a single
	 * operation.
	 * 
	 * @param fieldGroupValue
	 *            The fieldGroupValue this operationManager works for. An
	 *            operation will be created when a particle which matches the
	 *            fieldGroupValue arrives for the first time. If the
	 *            fieldGroupValue is null, only one instance of the operation
	 *            will be created directly within this constructor.
	 * @param batchOperationClass
	 *            The class of the single operation to be used.
	 * @param conf
	 *            A reference to the storm config
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public OperationManager(String fieldGroupValue,
			Class<? extends SingleOperation> operationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration)
			throws InstantiationException, IllegalAccessException {
		operationManager(fieldGroupValue, null, operationClass,
				stormNativeConfig, stormConfiguration);
	}

	/**
	 * An internal class to create the operationManager
	 * 
	 * @param fieldGroupValue
	 * @param batcherClass
	 * @param operationClass
	 * @param conf
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private void operationManager(String fieldGroupValue,
			Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration)
			throws InstantiationException, IllegalAccessException {
		this.fieldGrouperValue = fieldGroupValue;
		this.operationClass = operationClass;
		this.batcherClass = batcherClass;
		this.operationClass = operationClass;
		this.zookeeperStormConfiguration = stormConfiguration;
		metaParticleHandlers = new ArrayList<MetaParticleHandler>();

		if (fieldGroupValue == null) {
			createOperation(null);
		}
		logger.debug("Operation manager for fieldGroupValue" + fieldGroupValue
				+ " created");
	}

	/**
	 * Process a DataParticle
	 * 
	 * @param dataParticle
	 *            DataParticle to be processed. If the particle == null, null is
	 *            returned
	 * @return returns a list with zero or more DataParticles, these particles
	 *         should be emitted by the bolt. Or null in case of an error, which
	 *         will be logged
	 */
	public List<Particle> processDataParticle(DataParticle dataParticle) {
		if (dataParticle == null)
			return null;

		// make sure this operation manager has an operation and optional
		// metaParticleHandlers
		if (operation == null) {
			// this is the first particle, no operation has been instantiated.
			try {
				createOperation(dataParticle);
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("For fieldGroupValue " + fieldGrouperValue
						+ ": can not create the operation ("
						+ operationClass.getName() + ") msg=" + e);
				return null;
			}
		}

		try {
			List<DataParticle> outputDataParticles = null;

			// is there a batcher?
			if (batcher != null) {
				// batch dataParticle and give it to the batcherOperation
				List<DataParticleBatch> batchedParticles = batcher
						.batch(dataParticle);

				// are there one or more particle batches to be processed?
				if (batchedParticles != null) {
					outputDataParticles = new ArrayList<DataParticle>();
					for (DataParticleBatch batchedParticle : batchedParticles) {
						outputDataParticles.addAll(((BatchOperation) operation)
								.execute(batchedParticle));
					}
				}
			} else {
				// it is an operation without a batcher
				outputDataParticles = ((SingleOperation) operation)
						.execute(dataParticle);
			}

			// are there any particles to be emitted, convert into Particle
			// list?
			List<Particle> result = new ArrayList<Particle>();
			if (outputDataParticles != null) {
				result.addAll(outputDataParticles);
			}
			return result;
		} catch (BatcherException | OperationException e) {
			logger.error(
					"Unable to execute operation due to: " + e.getMessage(), e);
			return null;
		}
	}

	/**
	 * Process a MetaParticle
	 * 
	 * @param metaParticle
	 *            Particle to be processed. If the particle == null, null is
	 *            returned
	 * @return returns a list with one MetaParticle (to be sent further up to
	 *         the topology), zero or more DataParticles or null in case of an
	 *         error. These particles should be emitted by the bolt.
	 */
	public List<Particle> processMetaParticle(MetaParticle metaParticle) {
		if (metaParticle != null) {
			return handleMetaParticle(metaParticle);
		} else {
			return null;
		}
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

		long timestamp = 0;
		if (firstParticle != null) {
			timestamp = firstParticle.getTimestamp();
		}

		// create batcher if needed
		if (batcherClass != null)
			try {
				batcher = batcherClass.newInstance();
				batcher.init(fieldGrouperValue, timestamp, stormNativeConfig,
						zookeeperStormConfiguration);
			} catch (BatcherException e) {
				throw new InstantiationException(e.getMessage());
			}

		try {
			// create new operation and initialize it
			operation = operationClass.newInstance();

			// is it a BatchOperation?
			if (BatchOperation.class.isInstance(operation)) {
				((BatchOperation) operation).init(fieldGrouperValue,
						stormNativeConfig, zookeeperStormConfiguration);
				operation.prepareForFirstParticle(timestamp);

			}

			// or is it a SingleOperation?
			else if (SingleOperation.class.isInstance(operation)) {
				operation.init(fieldGrouperValue, stormNativeConfig,
						zookeeperStormConfiguration);
				operation.prepareForFirstParticle(timestamp);
			}

			// unknown operation type
			else {
				logger.error("Internal error: OperationClass of unknown type "
						+ operationClass.getName() + ", expected: "
						+ SingleOperation.class.getName() + " or "
						+ BatchOperation.class.getName());
			}
		} catch (OperationException oe) {
			// stop creating an operation, and inform the caller why
			throw new InstantiationException(
					"Unable to initialize operation due to: " + oe.getMessage());
		}

		// create the meta particle handlers related to this operation
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
	protected List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		List<Particle> result = new ArrayList<Particle>();
		for (MetaParticleHandler mph : metaParticleHandlers) {
			MetaParticleHandlerDecleration mphd = mph.getClass().getAnnotation(
					MetaParticleHandlerDecleration.class);
			if (metaParticle.getClass().isAssignableFrom(mphd.metaParticle())) {
				result.addAll(mph.handleMetaParticle(metaParticle));
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
		List<Class<? extends Particle>> result = new ArrayList<Class<? extends Particle>>();
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
		List<Class<? extends DataParticle>> result = new ArrayList<Class<? extends DataParticle>>();
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
		List<Class<? extends MetaParticle>> result = new ArrayList<Class<? extends MetaParticle>>();
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
