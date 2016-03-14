package com.razorthink.bigbrain.interpreter.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JSONUtils {
	private static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

	public static String toJson(Object obj) {
		return gson.toJson(obj);
	}

	public static <T> T parse(String json, Class<T> clazz) {
		return gson.fromJson(json, clazz);
	}

	public static void print(Object obj) {
		System.out.println(toJson(obj));
	}
}