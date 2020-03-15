package de.mklinger.blobstore.io;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.OutputStream;
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

import de.mklinger.blobstore.Methods;

@RunWith(Parameterized.class)
public class ProxyOutputStreamTest {
	// Types under test:
	private static final Class<OutputStream> delegateType = OutputStream.class;
	private static final Function<OutputStream, ProxyOutputStream> wrapperFactory = ProxyOutputStream::new;
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

	public ProxyOutputStreamTest(final Method method, final String methodDescription) {
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
		if (type == byte[].class) {
			return (T) new byte[0];
		} else if (type == int.class) {
			return (T) Integer.valueOf(0);
		} else {
			return mock(type);
		}
	}
}
