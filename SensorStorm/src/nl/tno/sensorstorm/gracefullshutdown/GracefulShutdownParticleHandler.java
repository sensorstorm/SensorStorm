package nl.tno.sensorstorm.gracefullshutdown;

import java.io.Serializable;
import java.util.List;

import nl.tno.sensorstorm.api.annotation.MetaParticleHandlerDeclaration;
import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.api.processing.MetaParticleHandler;
import nl.tno.sensorstorm.api.processing.Operation;

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
@MetaParticleHandlerDeclaration(metaParticle = GracefullShutdownParticle.class)
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
