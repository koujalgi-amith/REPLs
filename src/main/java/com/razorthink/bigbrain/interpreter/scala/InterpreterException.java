package com.razorthink.bigbrain.interpreter.scala;

public class InterpreterException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InterpreterException(Throwable e) {
		super(e);
	}

	public InterpreterException(String m) {
		super(m);
	}

}