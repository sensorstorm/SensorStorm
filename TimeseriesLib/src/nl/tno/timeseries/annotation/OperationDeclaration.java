package nl.tno.timeseries.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.MetaParticleHandler;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface OperationDeclaration {

	Class<? extends DataParticle>[] inputs();

	Class<? extends DataParticle>[] outputs() default {};

	Class<? extends MetaParticleHandler>[] metaParticleHandlers() default {};

}
