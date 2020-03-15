package de.mklinger.blobstore.io;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class LazyInputStreamTest {
	@Test
	@SuppressWarnings("resource")
	public void read1Test() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).read();
		verify(inputStream).read();
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void read2Test() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		final byte[] b = new byte[10];
		new LazyInputStream(() -> inputStream).read(b);
		verify(inputStream).read(b);
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void read3Test() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		final byte[] b = new byte[10];
		new LazyInputStream(() -> inputStream).read(b, 1, 2);
		verify(inputStream).read(b, 1, 2);
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void skipTest() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).skip(100);
		verify(inputStream).skip(100);
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void availableTest() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).available();
		verify(inputStream).available();
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	public void noCloseIfNotCreatedTest() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).close();
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	public void closeTest() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		final LazyInputStream lazyInputStream = new LazyInputStream(() -> inputStream);
		lazyInputStream.markSupported();
		verify(inputStream).markSupported();
		lazyInputStream.close();
		verify(inputStream).close();
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void markTest() {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).mark(10);
		verify(inputStream).mark(10);
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void resetTest() throws IOException {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).reset();
		verify(inputStream).reset();
		verifyNoMoreInteractions(inputStream);
	}

	@Test
	@SuppressWarnings("resource")
	public void markSupportedTest() {
		final InputStream inputStream = mock(InputStream.class);
		new LazyInputStream(() -> inputStream).markSupported();
		verify(inputStream).markSupported();
		verifyNoMoreInteractions(inputStream);
	}
}
