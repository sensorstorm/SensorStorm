package nl.tno.timeseries.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.timeseries.particles.Particle;

/**
 * Tell the mapper that this field should be present in a tuple. Only relevant
 * if the {@link Particle} doesn't have a custom mapper.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface TupleField {

	/**
	 * Name of this field in the tuple. If empty, the name of the field will be
	 * used
	 */
	String name() default "";

}
