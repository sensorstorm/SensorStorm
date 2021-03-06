package nl.tno.sensorstorm.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.Fetcher;

/**
 * Mandatory annotation for the {@link Fetcher}. The parameter outputs is an
 * array containing zero or more {@link DataParticle} which the {@link Fetcher}
 * promises to return.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FetcherDeclaration {
	/**
	 * List of types of {@link DataParticle}s produced by this {@link Fetcher}.
	 */
	Class<? extends DataParticle>[] outputs();
}
