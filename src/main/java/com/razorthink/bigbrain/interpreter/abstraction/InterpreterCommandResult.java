package com.razorthink.bigbrain.interpreter.abstraction;

import java.util.Date;

/**
 * Created by dev on 2/24/16.
 */
public class InterpreterCommandResult {
    private String result;
    private String error;
    private Exception stacktrace;
    private String command;
	private Date startedAt, endedAt;

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public Exception getStacktrace() {
		return stacktrace;
	}

	public void setStacktrace(Exception stacktrace) {
		this.stacktrace = stacktrace;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	public Date getEndedAt() {
		return endedAt;
	}

	public void setEndedAt(Date endedAt) {
		this.endedAt = endedAt;
	}
}