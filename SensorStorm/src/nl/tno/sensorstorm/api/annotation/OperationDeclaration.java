package nl.tno.sensorstorm.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.MetaParticleHandler;
import nl.tno.sensorstorm.api.processing.Operation;

/**
 * Mandatory annotation for the {@link Operation}. There can be three parameters
 * (inputs, outputs and metaParticleHandlers). The inputs is mandatory and the
 * other two are optional.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OperationDeclaration {
	/**
	 * An array containing one or more types of {@link DataParticle}s that this
	 * {@link Operation} can process.
	 */
	Class<? extends DataParticle>[] inputs();

	/**
	 * An array containing zero or more types of {@link DataParticle}s
	 * specifying the dataParticles this {@link Operation} produces.
	 */
	Class<? extends DataParticle>[] outputs() default {};

	/**
	 * An array containing zero or more {@link MetaParticleHandler}s that must
	 * be instantiated and connected to this operation.
	 */
	Class<? extends MetaParticleHandler>[] metaParticleHandlers() default {};

}
