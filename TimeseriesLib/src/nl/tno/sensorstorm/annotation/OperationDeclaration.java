package nl.tno.sensorstorm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.MetaParticleHandler;

/**
 * Annotation for the Operation. There can be three paramters (inputs, outputs
 * and metaParticleHandlers). The inputs is mandatory and the other two are
 * optional.
 * 
 * inputs: is an array containing one or more DataParticles specifying the
 * dataParticles must be passed to the operation as input.
 * 
 * outputs: is an array containing zero or more DataParticles specifying the
 * dataParticles the operation outputs.
 * 
 * metaParticleHandlers: is an array containing zero or more
 * MetaParticleHandlers that must be instantiated and connected to this
 * operation.
 * 
 * @author waaijbdvd
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface OperationDeclaration {

	Class<? extends DataParticle>[] inputs();

	Class<? extends DataParticle>[] outputs() default {};

	Class<? extends MetaParticleHandler>[] metaParticleHandlers() default {};

}
