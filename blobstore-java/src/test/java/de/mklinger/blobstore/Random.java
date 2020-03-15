package de.mklinger.blobstore;

import java.util.concurrent.ThreadLocalRandom;

public class Random {
	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	public static int getRandomInt(final int max) {
		return ThreadLocalRandom.current().nextInt(max - 1) + 1;
	}

	public static String getRandomString(final int len) {
		final char[] c = new char[len];
		for (int i = 0; i < c.length; i++) {
			c[i] = ALPHABET.charAt(ThreadLocalRandom.current().nextInt(ALPHABET.length()));
		}
		return new String(c);
	}
}
