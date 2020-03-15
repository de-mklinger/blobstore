package de.mklinger.blobstore;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
@RunWith(Parameterized.class)
public class UncheckedBlobStoreWriterTest {
	// Types under test:
	private static final Class<BlobStoreWriter> delegateType = BlobStoreWriter.class;
	private static final Function<BlobStoreWriter, UncheckedBlobStoreWriter> wrapperFactory = UncheckedBlobStoreWriter::new;
	// ----

	private static final Predicate<Method> isDeclaredInObject = method -> Arrays.asList(Object.class.getMethods()).contains(method);
	private static final Predicate<Method> isStatic = method -> (method.getModifiers() & Modifier.STATIC) != 0;

	@Parameters(name= "{index}: {1}")
	public static Iterable<Object[]> methods() {
		return Arrays.stream(delegateType.getMethods())
				.filter(not(isDeclaredInObject))
				.filter(not(isStatic))
				.map(method -> new Object[] { method, Methods.describe(method) })
				.collect(toList());
	}

	private final Method method;

	public UncheckedBlobStoreWriterTest(final Method method, final String methodDescription) {
		this.method = method;
	}

	@Test
	public void testDelegation() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		final Object delegate = mock(delegateType);
		final Object wrapper = wrapperFactory.apply(delegateType.cast(delegate));

		final Object[] args = Arrays.stream(method.getParameterTypes())
				.map(this::newArg)
				.toArray();

		method.invoke(wrapper, args);

		method.invoke(verify(delegate), args);
	}

	@SuppressWarnings("unchecked")
	private <T> T newArg(Class<T> type) {
		if (type == String.class) {
			return (T) "";
		} else {
			return mock(type);
		}
	}

	@Test
	public void testUncheckedThrowing() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (!Arrays.asList(method.getExceptionTypes()).contains(IOException.class)) {
			return;
		}

		final BlobStoreWriter delegate = mock(BlobStoreWriter.class, _unused -> {
			throw new IOException("test");
		});

		final UncheckedBlobStoreWriter wrapper = new UncheckedBlobStoreWriter(delegate);

		final Object[] args = Arrays.stream(method.getParameterTypes())
				.map(type -> null)
				.toArray();

		try {
			method.invoke(wrapper, args);
			fail("Expected RuntimeException, but no Exception thrown");
		} catch (final UncheckedIOException e) {
			// expected, but not thrown by Mockito
		} catch (final InvocationTargetException e) {
			if (e.getTargetException() != null && UncheckedIOException.class == e.getTargetException().getClass()) {
				// expected with Mockito
			} else {
				throw new AssertionError("Expected UncheckedIOException, but " + e.getClass().getSimpleName() + " was thrown", e);
			}
		} catch (final Throwable e) {
			throw new AssertionError("Expected UncheckedIOException, but " + e.getClass().getSimpleName() + " was thrown", e);
		}

		method.invoke(verify(delegate), args);
	}
}
