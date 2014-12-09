package nl.tno.timeseries.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.timeseries.particles.DataParticle;

/**
 * Annotation for the Fetcher. The parameter outputs is an array containing zero
 * or more DataParticles which the Fetcher promises to return.
 * 
 * @author waaijbdvd
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface FetcherDeclaration {
	Class<? extends DataParticle>[] outputs();
}
