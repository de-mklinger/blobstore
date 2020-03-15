package de.mklinger.blobstore.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.mklinger.blobstore.BlobStoreFormatException;
import de.mklinger.blobstore.FileBlobStoreReader;
import de.mklinger.blobstore.FileBlobStoreWriter;

public class MergeBlobs {
	public static void main(final String[] args) {
		if (args.length < 3) {
			usage();
			System.exit(1);
		}

		try {
			merge(args);
		} catch (final Exception e) {
			System.err.println("Error: " + e.toString());
			usage();
			System.exit(1);
		}
	}

	private static void merge(final String[] args) throws IOException {
		final List<File> inputFiles = new ArrayList<>(args.length - 1);
		for (int i = 0; i < args.length - 1; i++) {
			final File inputFile = new File(args[i]);
			if (!inputFile.exists()) {
				throw new FileNotFoundException("Input file " + inputFile.getAbsolutePath() + " not found");
			}
			inputFiles.add(inputFile);
		}

		final File outputFile = new File(args[args.length - 1]);
		if (outputFile.exists()) {
			throw new IOException("Output file " + outputFile.getAbsolutePath() + " already exists");
		}

		System.err.println("Output to: " + outputFile.getAbsolutePath());

		try (FileBlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(outputFile)
				.overwrite(true)
				.build()) {

			for (final File inputFile : inputFiles) {
				try {
					final FileBlobStoreReader reader = new FileBlobStoreReader(inputFile);
					System.err.println("Merging: " + inputFile.getAbsolutePath());
					writer.mergeFrom(reader);
				} catch (final BlobStoreFormatException e) {
					System.err.println("NOT Merging: " + inputFile.getAbsolutePath() + ". Seems to be broken");
				}
			}
		}

		System.err.println("Done.");
	}

	private static void usage() {
		System.err.println("Usage: " + MergeBlobs.class.getSimpleName() + " <inputfile1> <inputfile2> [...] <outputfile>");
	}
}
