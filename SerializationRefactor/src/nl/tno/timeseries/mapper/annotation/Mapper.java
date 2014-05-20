package nl.tno.timeseries.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Mapper {

	/**
	 * Defines the object as particle
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	Class<? extends CustomParticlePojoMapper> value();

}
