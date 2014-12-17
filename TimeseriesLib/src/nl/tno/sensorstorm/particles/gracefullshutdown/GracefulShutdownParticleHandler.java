package nl.tno.sensorstorm.particles.gracefullshutdown;

import java.io.Serializable;
import java.util.List;

import nl.tno.sensorstorm.annotation.MetaParticleHandlerDecleration;
import nl.tno.sensorstorm.operations.Operation;
import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.MetaParticleHandler;
import nl.tno.sensorstorm.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to process the gracefullShutdown meat particle. It will inform all
 * registered operations. Registration goes via the @OperationDeclaration
 * annotation, field metaParticleHandlers.
 * 
 * @author waaijbdvd
 * 
 */
@MetaParticleHandlerDecleration(metaParticle = GracefullShutdownParticle.class)
public class GracefulShutdownParticleHandler implements MetaParticleHandler,
		Serializable {
	protected Logger logger = LoggerFactory
			.getLogger(GracefulShutdownParticleHandler.class);
	private static final long serialVersionUID = 424494087672718473L;
	private GracefullShutdownInterface graceFullShutdownOperation;

	@Override
	public void init(Operation operation) {

		if (operation instanceof GracefullShutdownInterface) {
			graceFullShutdownOperation = (GracefullShutdownInterface) operation;
		} else {
			logger.error("Operation "
					+ operation.getClass().getName()
					+ " can not be connected to the gracefullShutdown. It does not implements the GracefullShutdownInterface");
		}

	}

	@Override
	public List<Particle> handleMetaParticle(MetaParticle metaParticle) {
		if (metaParticle instanceof GracefullShutdownParticle) {
			graceFullShutdownOperation.gracefullShutdown();
		}
		return null;
	}
}
