package nl.tno.sensorstorm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.particles.DataParticle;

/**
 * Annotation for the ChannelGrouper. The parameter outputs is an array
 * containing zero or more DataParticles which the ChannelGrouper promises to
 * produce.
 * 
 * @author waaijbdvd
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface ChannelGrouperDeclaration {
	Class<? extends DataParticle>[] outputs();
}
