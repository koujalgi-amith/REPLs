package com.razorthink.bigbrain.interpreter.test;

import com.razorthink.bigbrain.interpreter.abstraction.BrainInterpreter;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterManager;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterResultListener;
import com.razorthink.bigbrain.interpreter.abstraction.InterpreterType;
import com.razorthink.bigbrain.interpreter.util.Configuration;

public class IntpTest {
	public static void main(String[] args) throws Exception {
		Configuration.ENVIRONMENT = "amith";
		BrainInterpreter i = InterpreterManager.getInstance().getInterpreter(InterpreterType.SCALA_SPARK);
		i.attachListener(new InterpreterResultListener() {
			@Override
			public void onResult(String line) {
				System.out.println("RESULT LISTENER: " + line);
			}
		});
		i.start();
		i.interpret("println(\"Hi\")");
	}
}
