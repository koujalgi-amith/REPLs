package com.razorthink.bigbrain.interpreter.abstraction;

import java.util.Date;

public class InterpreterInfo {
	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setType(InterpreterType type) {
		this.type = type;
	}

	private InterpreterType type;
	private Date startedAt;
	private String host, id;

	public InterpreterType getType() {
		return type;
	}
}
