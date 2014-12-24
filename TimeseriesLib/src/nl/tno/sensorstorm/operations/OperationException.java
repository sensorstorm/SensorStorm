package nl.tno.sensorstorm.operations;

/**
 * Special type of {@link Exception} for when an error occurs in the Operation.
 */
public class OperationException extends Exception {

	private static final long serialVersionUID = -8326816195074071591L;

	/**
	 * Construct an empty {@link OperationException}.
	 */
	public OperationException() {
		super();
	}

	/**
	 * Construct a {@link OperationException} with a message.
	 * 
	 * @param message
	 *            Message of the {@link Exception}
	 */
	public OperationException(String message) {
		super(message);
	}

	/**
	 * Construct a {@link OperationException} with a {@link Throwable} cause.
	 * 
	 * @param cause
	 *            Cause of this {@link Exception}
	 */
	public OperationException(Throwable cause) {
		super(cause);
	}

	/**
	 * Construct a {@link OperationException} with a message and
	 * {@link Throwable} cause.
	 * 
	 * @param message
	 *            Message of the {@link Exception}
	 * @param cause
	 *            Cause of this {@link Exception}
	 */
	public OperationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Construct a {@link OperationException} with a message and
	 * {@link Throwable} cause.
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
	public OperationException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
