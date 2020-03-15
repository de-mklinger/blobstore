<?php

function serveBlobEntry($blobstore, $f, $blobEntry) {
	global $config;
	
	list($offset, $length, $encoding, $mediaType) = $blobEntry;
	
	if (!$mediaType) {
		$mediaType = $config["defaults"]["mediaType"];
	}
	
	if (!$encoding) {
	    $encoding = $config["defaults"]["encoding"];
	}
	
	$unzip = false;
	if ($encoding == "gzip" && !isGzipSupported()) {
		$unzip = true;
		$encoding = "identity";
	}
	
	header("Content-Type: " . $mediaType);
	header("Content-Encoding: " . $encoding);
	if (!$unzip) {
		header("Content-Length: " . $length);
	}
	
	$rfc_1123_date = gmdate('D, d M Y H:i:s T', $blobstore["mtime"]);
	header("Last-Modified: " . $rfc_1123_date);
	
	if ($unzip) {
		$tmpFile = tempnam("/tmp", "blobunzip");
		header("X-tmp: $tmpFile");
		$out = fopen($tmpFile, "w");
	} else {
		$out = fopen("php://output", "w");
	}
		
	$bufSize = 4096;
	fseek($f, $offset);
	while ($length > 0) {
		$readLength = min($bufSize, $length);
		$buf = fread($f, $readLength);
		$actualReadLength = strlen($buf);
		if ($actualReadLength == 0) {
			// ERROR
		}
		fwrite($out, $buf, $actualReadLength);
		$length -= $actualReadLength;
	}
	
	if ($unzip) {
		header("X-Mod: unzip");
		fclose($out);
		readgzfile($tmpFile);
		unlink($tmpFile);
	}
}

function blobStoreReaderMain() {
	global $config;
	
	header("Vary: Accept-Encoding, Accept");
	
	$blobstore = $config["getBlobstore"]();
	if (!$blobstore) {
	    serve404("Blobstore not found");
	}
	$blobFilepath = $blobstore["filepath"];
	if (!file_exists($blobFilepath)) {
	    serve404("Blobstore not found");
	}
	$name = getEntryName();
	if (!$name) {
		serve400("Missing entry name");
	}
	
	$blobFile = fopen($blobFilepath, "r");
	if (!$blobFile) {
		serve500();
	}
	$blobEntry = searchBlobEntry($blobFile, $name);
	if (!$blobEntry) {
		serve404("Entry not found: '" . $name . "'");
	}
	serveBlobEntry($blobstore, $blobFile, $blobEntry);
	fclose($blobFile);
	exit;
}

?>