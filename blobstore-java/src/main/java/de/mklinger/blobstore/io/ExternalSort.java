package de.mklinger.blobstore.io;

// filename: ExternalSort.java
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import de.mklinger.micro.closeables.Closeables;

/**
 * Goal: offer a generic external-memory sorting program in Java.
 *
 * It must be : - hackable (easy to adapt) - scalable to large files - sensibly
 * efficient.
 *
 * This software is in the public domain.
 *
 * Usage: java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 *
 * You can change the default maximal number of temporary files with the -t
 * flag: java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 * -t 3
 *
 * For very large files, you might want to use an appropriate flag to allocate
 * more memory to the Java VM: java -Xms2G
 * com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 *
 * By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 * Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Sugumaran Harikrishnan,
 * Thomas, Mueller, Jerry Yang, First published: April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java/
 *
 * mklinger: Reduced to the functionality required for blobstore.
 */
public class ExternalSort {

	private static final char UNIX_NEW_LINE = '\n';

	/**
	 * This merges several BinaryFileBuffer to an output writer.
	 *
	 * @param fbw
	 *                A buffer where we write the data.
	 * @param cmp
	 *                A comparator object that tells us how to sort the
	 *                lines.
	 * @param distinct
	 *                Pass <code>true</code> if duplicate lines should be
	 *                discarded. (elchetz@gmail.com)
	 * @param buffers
	 *                Where the data should be read.
	 * @return The number of lines sorted. (P. Beaudoin)
	 * @throws IOException
	 *
	 */
	public static int mergeSortedFiles(final BufferedWriter fbw,
			final Comparator<String> cmp, final boolean distinct,
			final List<BinaryFileBuffer> buffers) throws IOException {
		final PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
				11, new Comparator<BinaryFileBuffer>() {
					@Override
					public int compare(final BinaryFileBuffer i,
							final BinaryFileBuffer j) {
						return cmp.compare(i.peek(), j.peek());
					}
				});
		for (final BinaryFileBuffer bfb : buffers) {
			if (!bfb.empty()) {
				pq.add(bfb);
			}
		}
		int rowcounter = 0;
		String lastLine = null;
		try {
			while (pq.size() > 0) {
				final BinaryFileBuffer bfb = pq.poll();
				final String r = bfb.pop();
				// Skip duplicate lines
				if (!distinct || !r.equals(lastLine)) {
					fbw.write(r);
					fbw.write(UNIX_NEW_LINE);
					lastLine = r;
				}
				++rowcounter;
				if (bfb.empty()) {
					bfb.fbr.close();
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally {
			fbw.close();
			for (final BinaryFileBuffer bfb : pq) {
				bfb.close();
			}
		}
		return rowcounter;

	}

	/**
	 * This merges a bunch of temporary flat files
	 *
	 * @param files
	 *                The {@link List} of sorted {@link File}s to be merged.
	 * @param distinct
	 *                Pass <code>true</code> if duplicate lines should be
	 *                discarded. (elchetz@gmail.com)
	 * @param writer
	 *                The output writer to merge the results to.
	 * @param cmp
	 *                The {@link Comparator} to use to compare
	 *                {@link String}s.
	 * @param cs
	 *                The {@link Charset} to be used for the byte to
	 *                character conversion.
	 * @param usegzip
	 *                assumes we used gzip compression for temporary files
	 * @return The number of lines sorted. (P. Beaudoin)
	 * @throws IOException
	 * @since leong
	 */
	public static int mergeSortedFiles(final List<File> files, final BufferedWriter fbw,
			final Comparator<String> cmp, final Charset cs, final boolean distinct,
			final boolean usegzip) throws IOException {
		final ArrayList<BinaryFileBuffer> bfbs = new ArrayList<BinaryFileBuffer>();
		for (final File f : files) {
			final int BUFFERSIZE = 2048;
			final InputStream in = new FileInputStream(f);
			BufferedReader br;
			if (usegzip) {
				br = new BufferedReader(
						new InputStreamReader(
								new GZIPInputStream(in,
										BUFFERSIZE), cs));
			} else {
				br = new BufferedReader(new InputStreamReader(
						in, cs));
			}

			final BinaryFileBuffer bfb = new BinaryFileBuffer(br);
			bfbs.add(bfb);
		}
		final int rowcounter = mergeSortedFiles(fbw, cmp, distinct, bfbs);
		for (final File f : files) {
			f.delete();
		}

		Closeables.closeUnchecked(bfbs);

		return rowcounter;
	}

	/**
	 * This is essentially a thin wrapper on top of a BufferedReader... which keeps
	 * the last line in memory.
	 *
	 * @author Daniel Lemire
	 */
	private static class BinaryFileBuffer implements Closeable {
		public BinaryFileBuffer(final BufferedReader r) throws IOException {
			this.fbr = r;
			reload();
		}

		@Override
		public void close() throws IOException {
			this.fbr.close();
		}

		public boolean empty() {
			return this.cache == null;
		}

		public String peek() {
			return this.cache;
		}

		public String pop() throws IOException {
			final String answer = peek().toString();// make a copy
			reload();
			return answer;
		}

		private void reload() throws IOException {
			this.cache = this.fbr.readLine();
		}

		public BufferedReader fbr;

		private String cache;

	}
}