package com.razorthink.bigbrain.interpreter.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import com.razorthink.bigbrain.interpreter.abstraction.BrainInterpreter;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterCommandResult;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterInfo;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterResultListener;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterType;
import com.razorthink.bigbrain.interpreter.interpreters.SparkScalaInterpreter;
import com.razorthink.bigbrain.interpreter.scala.InterpreterResult;
import com.razorthink.bigbrain.interpreter.scala.ResultListener;

public class ScalaInterpreter extends BrainInterpreter {

	private SparkScalaInterpreter intp;

	public ScalaInterpreter(Properties cfg) {
		super(cfg);
	}

	@Override
	public void attachListener(InterpreterResultListener listener) {
		intp.addListener(new ResultListener() {

			@Override
			public void onUpdate(String str) {
				listener.onResult(str);
			}
		});
	}

	@Override
	public void startup() {
		intp.start();
	}

	@Override
	public void shutdown() {
		intp.kill();
	}

	@Override
	public InterpreterCommandResult interpret(String cmd) {
		InterpreterCommandResult cmdRes = new InterpreterCommandResult();
		cmdRes.setStartedAt(new Date(System.currentTimeMillis()));
		InterpreterResult res = intp.interpret(cmd);
		cmdRes.setCommand(cmd);
		cmdRes.setEndedAt(new Date(System.currentTimeMillis()));
		cmdRes.setResult(res.getResult());
		return cmdRes;
	}

	@Override
	protected void initialise() {
		intp = new SparkScalaInterpreter(getCfg());
		intp.init();
		InterpreterInfo info = new InterpreterInfo();
		info.setType(InterpreterType.SCALA_SPARK);
		info.setId(UUID.randomUUID().toString());
		info.setStartedAt(new Date(System.currentTimeMillis()));
		try {
			info.setHost(InetAddress.getByName(null).getHostAddress());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		super.setInterpreterInfo(info);
	}
}
