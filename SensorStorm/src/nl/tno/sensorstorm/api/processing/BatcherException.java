package nl.tno.sensorstorm.api.processing;

/**
 * Special type of {@link Exception} for when an error occurs in the batching
 * process.
 */
public class BatcherException extends Exception {

	private static final long serialVersionUID = 6382103260176369288L;

	/**
	 * Construct an empty {@link BatcherException}.
	 */
	public BatcherException() {
		super();
	}

	/**
	 * Construct a {@link BatcherException} with a message.
	 * 
	 * @param message
	 *            Message of the {@link Exception}
	 */
	public BatcherException(String message) {
		super(message);
	}

	/**
	 * Construct a {@link BatcherException} with a {@link Throwable} cause.
	 * 
	 * @param cause
	 *            Cause of this {@link Exception}
	 */
	public BatcherException(Throwable cause) {
		super(cause);
	}

	/**
	 * Construct a {@link BatcherException} with a message and {@link Throwable}
	 * cause.
	 * 
	 * @param message
	 *            Message of the {@link Exception}
	 * @param cause
	 *            Cause of this {@link Exception}
	 */
	public BatcherException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Construct a {@link BatcherException} with a message and {@link Throwable}
	 * cause.
	 * 
	 * @param message
	 *            the detail message.
	 * @param cause
	 *            the cause. (A {@code null} value is permitted, and indicates
	 *            that the cause is nonexistent or unknown.)
	 * @param enableSuppression
	 *            whether or not suppression is enabled or disabled
	 * @param writableStackTrace
	 *            whether or not the stack trace should be writable
	 */
	public BatcherException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
