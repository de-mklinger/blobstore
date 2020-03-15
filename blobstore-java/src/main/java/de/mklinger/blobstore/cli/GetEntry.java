package de.mklinger.blobstore.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import de.mklinger.blobstore.BlobEntry;
import de.mklinger.blobstore.BlobStoreReader;
import de.mklinger.blobstore.MultiFileBlobStoreReader;
import de.mklinger.micro.streamcopy.StreamCopy;

public class GetEntry {
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println(String.format("Usage: %s <name> <blob-file> [<blob-file> ...]", GetEntry.class.getName()));
			System.exit(1);
		}

		final String name = args[0];

		final List<File> files = Stream.of(args)
				.skip(1)
				.map(File::new)
				.collect(Collectors.toList());

		final BlobStoreReader reader = new MultiFileBlobStoreReader(files);

		final BlobEntry blobEntry = reader.getBlobEntry(name);

		if (blobEntry == null) {
			System.err.println("Not found: '" + name + "'");
			System.exit(1);
		}

		if (blobEntry.getEncoding() == BlobEntry.ENCODING_GZIP) {
			try (InputStream blobEntryContents = new GZIPInputStream(reader.getBlobEntryContents(blobEntry))) {
				StreamCopy.copy(blobEntryContents, System.out);
			}
		} else {
			try (InputStream blobEntryContents = reader.getBlobEntryContents(blobEntry)) {
				StreamCopy.copy(blobEntryContents, System.out);
			}
		}

	}
}
