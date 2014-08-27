package nl.tno.timeseries.gracefullshutdown;

import java.io.Serializable;
import java.util.List;

import nl.tno.timeseries.annotation.MetaParticleHandlerDecleration;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.particles.MetaParticleHandler;

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
