package com.razorthink.bigbrain.interpreter.app;

import java.util.List;
import java.util.Properties;

import com.razorthink.bigbrain.interpreter.interpreters.SparkScalaInterpreter;
import com.razorthink.bigbrain.interpreter.scala.InterpreterResult;
import com.razorthink.bigbrain.interpreter.util.Configuration;
import com.razorthink.bigbrain.interpreter.util.JSONUtils;

public class App {
	public static void main(String[] args) throws InterruptedException {
		Configuration.ENVIRONMENT = "amith";
		Exec e1 = new Exec();
		e1.start();
	}
}

class Exec extends Thread {
	@Override
	public void run() {
		SparkScalaInterpreter interpreter = new SparkScalaInterpreter(new Properties());
		interpreter.start();
		// interpreter.interpret("case class message(name : String)");
		// interpreter.interpret("println(\"Hello\")");
		// interpreter.interpret("val p = message(\"Hello World\")");
		// interpreter.interpret("p.name");
		// interpreter.interpret("def toUpper(x : String) : String =
		// x.toUpperCase");
		// interpreter.interpret("toUpper(p.name)");
		// interpreter.interpret("def square(x : Int) : Int = x * x;
		// square(10)");
		// interpreter.interpret("sc.toString()");
		// interpreter.interpret("case class Person(name:String, age:Int)\n");
		// interpreter.interpret(
		// "val people = sc.parallelize(Seq(Person(\"moon\", 33),
		// Person(\"jobs\", 51), Person(\"gates\", 51), Person(\"park\",
		// 34)))\n");
		// interpreter.interpret("people.take(3)");
		// interpreter.interpret("import scala.collection.mutable.ArrayBuffer");
		// InterpreterResult res = interpreter.interpret("val p =
		// println(\"Hello World\")");
		// InterpreterResult res = interpreter.interpret("println(\"12\",
		// \"21\")");
		// System.out.println("\n\n\n\n\n" + res.getResult() + "\n\n\n\n\n");

		List<InterpreterResult> results = interpreter.interpret(new String[] { "case class message(name : String)",
				"println(\"Hello\")", "val p = message(\"Hello World\")" });
		// for (InterpreterResult r : results) {
		// System.out.println(r.getResult());
		// }
		JSONUtils.print(results);
		interpreter.kill();
	}
}