package de.mklinger.blobstore;

import java.lang.reflect.Method;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class Methods {
	public static String describe(final Method m) {
		final StringBuilder sb = new StringBuilder();
		sb.append(m.getName());
		sb.append("(");
		boolean firstArg = true;
		for (final Class<?> type : m.getParameterTypes()) {
			if (!firstArg) {
				sb.append(", ");
			}
			sb.append(type.getSimpleName());
			firstArg = false;
		}
		sb.append(")");
		return sb.toString();
	}

}
