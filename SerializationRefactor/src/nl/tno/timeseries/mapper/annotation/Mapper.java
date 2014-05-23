package nl.tno.timeseries.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;

/**
 * Defines a custom mapper for a Particle class
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Mapper {

	@SuppressWarnings("rawtypes")
	Class<? extends CustomParticlePojoMapper> value();

}
