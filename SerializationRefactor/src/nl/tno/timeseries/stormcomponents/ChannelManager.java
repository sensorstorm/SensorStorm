package nl.tno.timeseries.stormcomponents;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.MetaParticleHandler;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;

public class ChannelManager implements Serializable {

	private static final long serialVersionUID = 3141072548366321818L;

	private String channelId;
	private Operation operation;
	private Batcher batcher;
	private List<MetaParticleHandler> metaParticleHandlers;
	private Class<? extends Operation> operationClass;

	@SuppressWarnings("rawtypes")
	private Map conf;

	public ChannelManager(String channelId, Class<? extends Batcher> batcherClass,
			Class<? extends Operation> operationClass, @SuppressWarnings("rawtypes") Map conf) {
		this.operationClass = operationClass;
		this.conf = conf;
		this.channelId = channelId;
	}

	private void createOperation(Particle firstParticle) {
		try {
			// TODO create metaParticleHandlers
			operation = operationClass.newInstance();
			operation.init(firstParticle.getChannelId(), firstParticle.getSequenceNr(), conf);
			createMetaParticleHandlers(operation);
		} catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void createMetaParticleHandlers(Operation operation) throws InstantiationException, IllegalAccessException {
		for (Annotation a : operation.getClass().getAnnotations()) {
			if (a instanceof OperationDeclaration) {
				for (Class<? extends MetaParticleHandler> mph : ((OperationDeclaration) a).metaParticleHandlers()) {
					MetaParticleHandler newInstance = mph.newInstance();
					newInstance.init(operation);
					metaParticleHandlers.add(newInstance);
				}
			}
		}
	}

	public List<Particle> processParticles(List<Particle> list) {
		if (operation == null) {
			createOperation(list.get(0));
		}
		return null;
	}

	public static List<Class<? extends Particle>> getOutputParticles(Class<? extends Operation> operationClass)
			throws NullPointerException {
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
