package nl.tno.sensorstorm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.MetaParticleHandler;

/**
 * Annotation for the {@link MetaParticleHandler}. The parameter metaParticle
 * specifies for which metaParticle the {@link MetaParticleHandler} wants to
 * register.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface MetaParticleHandlerDeclaration {
	/**
	 * The types of {@link MetaParticle}s this {@link MetaParticleHandler}
	 * produces.
	 */
	Class<? extends MetaParticle> metaParticle();

}
