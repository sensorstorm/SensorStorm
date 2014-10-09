package nl.tno.stormcv.util.adaptor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;

/**
 * FileAdaptor implementations are used to get/put files from/to remote locations. Using the moveTo(...) function the 
 * adaptor moves to a remote location after which it can either download the file to the local filesystem (most likely /tmp)
 * or upload a local file to that location. The right adaptor can be fetched from the @link {@link AdaptorHolder} by providing 
 * the location you want to interact with. This location must start with a prefix/protocol (file, ftp, s3, ...) and the
 * adaptor supporting that protocol must be registered in the StormCV configuration.
 * 
 * @author Corne Versloot
 *
 */
public interface FileAdaptor extends Serializable{

	@SuppressWarnings("rawtypes")
	/**
	 * Prepare can be called to get storm configuration in FileAdaptors (typically performed within prepare functions)
	 * @param stormConf
	 */
	public void prepare(Map stormConf) throws DatatypeConfigurationException;
	
	/**
	 * Specify the valid extensions to use by this FileLocation (used by list).
	 * If no extensions are set all files will match 
	 * @param extensions
	 * @return itself
	 */
	public FileAdaptor setExtensions(String[] extensions);
	
	/**
	 * Moves the remote location to the specified point 
	 * @param location
	 */
	public void moveTo(String location) throws IOException;
	
	/**
	 * Copies the specified local file to the currently set (remote) location. 
	 * If the remote location is a 'directory' it will copy the file into the directory.
	 * If the remote location points to a file the remote file will likely be overwritten 
	 * (or an exception is thrown).
	 * @param localFile
	 * @param delete indicates if the localFile must be deleted after a successful copy
	 * @throws IOException
	 */
	public void copyFile(File localFile, boolean delete) throws IOException;
	
	/**
	 * List all 'files' within the current location which match the provided extensions.
	 * If the extensions is null than all files will be returned.
	 * @return
	 */
	public List<String> list();
	
	/**
	 * Returns the FileLocation's prefix (ftp, s3, file, ...)
	 * @return the protocol this adaptor supports
	 */
	public String getProtocol();
	
	/**
	 * Gets the current remote location as a file ( triggers a download )
	 * @return the local File the remote content was downloaded to
	 */
	public File getAsFile() throws IOException;
	 
	/**
	 * Gets the current location as a stream (i.e. no download)
	 * @return the remote location as a byte stream
	 */
	public InputStream getAsStream() throws IOException;
	
	/**
	 * Makes a deep copy of this object 
	 * @return a deep copy of itself
	 */
	public FileAdaptor deepCopy();
	
}