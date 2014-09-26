package nl.tno.stormcv.operation;

import java.io.IOException;

import nl.tno.stormcv.util.NativeUtils;

/**
 * Abstract class containing basic functionality to load (custom) OpenCV libraries. The name of the library to be loaded
 * can be set though the libName function. If this name is not set it will attempt to find and load the right OpenCV 
 * library automatically. It is advised to set the name of the library to use.
 * 
 * The name of the library can be configured globally within the Storm Config with key LIBNAME_CONFIG_KEY
 * 
 * @author Corne Versloot
 *
 */
public abstract class OpenCVOperation {

	public static final String LIBNAME_CONFIG_KEY = "opencv.lib.name";

	private String libName;
	
	public OpenCVOperation libName(String name){
		this.libName = name;
		return this;
	}
	
	protected String getLibName(){
		return this.libName;
	}
	
	protected void loadOpenCV() throws RuntimeException, IOException{
		if(libName == null) NativeUtils.load();
		else NativeUtils.load(libName);
	}
	
	
}
