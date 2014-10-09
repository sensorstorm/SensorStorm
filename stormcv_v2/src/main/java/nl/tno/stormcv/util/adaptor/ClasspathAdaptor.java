package nl.tno.stormcv.util.adaptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;

public class ClasspathAdaptor implements FileAdaptor {

	private static final long serialVersionUID = 5841328581874552903L;
	public static String SCHEMA = "classpath"; 
	private URI location;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf) throws DatatypeConfigurationException {	}

	/**
	 * <b>Does not apply to classpath adaptor (since list does not function)</b>
	 */
	@Override
	public FileAdaptor setExtensions(String[] extensions) {
		return this;
	}

	@Override
	public void moveTo(String location) throws IOException{
		try {
			this.location = new URI(location);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	/**
	 * <b>Does not apply to classpath adaptor</b>
	 */
	@Override
	public void copyFile(File localFile, boolean delete) throws IOException {
		throw new IOException("Unable to copy file into jar!");

	}

	/**
	 * <b>Does not apply to classpath adaptor</b>
	 */
	@Override
	public List<String> list() {
		return new ArrayList<String>();
	}

	@Override
	public String getProtocol() {
		return SCHEMA;
	}

	/**
	 * Gets the file fromm the classpath, either as the direct file or extracted from a jar file
	 */
	@Override
	public File getAsFile() throws IOException {
		String path = "/"+location.toString();
		path = path.substring(path.lastIndexOf('/'));
		
		// check if and where the file is on the classpath
		URL url = getClass().getResource(path);
    	if(url == null) throw new FileNotFoundException("Unable to locate "+path);
    	if(url.getProtocol().equals("file")){
    		return new File(url.getFile());
    	}else if(!url.getProtocol().equals("jar")){
    		throw new IOException("Unknown protocol "+url.getProtocol()+" (required to get "+location+")");
    	}

        // Obtain filename from path
        String[] parts = path.split("/");
        String filename = (parts.length > 1) ? parts[parts.length - 1] : null;
 
        // Split filename to prexif and suffix (extension)
        String prefix = "";
        String suffix = null;
        if (filename != null) {
            parts = filename.split("\\.", 2);
            prefix = parts[0];
            suffix = (parts.length > 1) ? "."+parts[parts.length - 1] : null; // Thanks, davs! :-)
        }
 
        // Check if the filename is okay
        if (filename == null || prefix.length() < 3) {
            throw new IllegalArgumentException("The filename has to be at least 3 characters long.");
        }
 
        // Prepare temporary file
        File temp = File.createTempFile(prefix, suffix);
        if (!temp.exists()) {
            throw new FileNotFoundException("File " + temp.getAbsolutePath() + " does not exist.");
        }
 
        // Prepare buffer for data copying
        byte[] buffer = new byte[1024];
        int readBytes;
 
        // Open and check input stream
        InputStream is = getClass().getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("File " + path + " was not found inside JAR.");
        }
 
        // Open output stream and copy data between source file in JAR and the temporary file
        OutputStream os = new FileOutputStream(temp);
        try {
            while ((readBytes = is.read(buffer)) != -1) {
                os.write(buffer, 0, readBytes);
            }
        } finally {
            // If read/write fails, close streams safely before throwing an exception
            os.close();
            is.close();
        }
        
        return temp;
	}

	/**
	 * Returns a FileInputStream from the current location
	 */
	@Override
	public InputStream getAsStream() throws IOException {
		return new FileInputStream(this.getAsFile());
	}

	@Override
	public FileAdaptor deepCopy() {
		ClasspathAdaptor adaptor = new ClasspathAdaptor();
		adaptor.moveTo(location);
		return adaptor;
	}
	
	private void moveTo(URI uri){
		this.location = uri;
	}

}
