package nl.tno.sensorstorm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.particles.MetaParticle;

/**
 * Annotation for the MetaParticleHandler. The parameter metaParticle specifies
 * for which metaParticle the MetaParticleHandler wants to register.
 * 
 * @author waaijbdvd
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface MetaParticleHandlerDecleration {

	Class<? extends MetaParticle> metaParticle();

}
