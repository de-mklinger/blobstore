<?php

/** Browse Mode. **/
function isBrowseMode() {
	if (getEntryName()) {
		return false;
	} 
	return getBestSupportedMimeType(array("text/html", "application/xhtml+xml")) != null;
}

function render($template, $viewModel = NULL) {
    include("blobstorereader-browse-tpl-" . $template . ".php");
}

/** Browse Mode. **/
function serveHtmlHeader() {
    render("header");
}

/** Browse Mode. **/
function serveHtmlFooter() {
    render("footer");
}

/** Browse Mode. **/
function getHumanReadableSize($sizeInBytes) {
	$kb = 1024;
	$mb = $kb * 1024;
	$gb = $mb * 1024;
	if ($sizeInBytes > $gb) {
		return round($sizeInBytes / $gb, 2) . " GiB"; 
	}
	if ($sizeInBytes > $mb) {
		return round($sizeInBytes / $mb, 2) . " MiB"; 
	}
	if ($sizeInBytes > $kb) {
		return round($sizeInBytes / $kb, 2) . " KiB"; 
	}
	return $sizeInBytes . " B";
}

/** Browse Mode. **/
function serveBlobstoreList() {
	global $config;
	$files = $config["listBlobstores"]();
	
	function cmp($a, $b) {
		return strcmp($a["label"], $b["label"]);
	}
	usort($files, "cmp");

	serveHtmlHeader();
	render("blobstorelist", array("blobstores" => $files));
	serveHtmlFooter();
}

/** Browse Mode. **/
function serveSearchForm($blobstore) {
	serveHtmlHeader();
	render("searchform", array("blobstore" => $blobstore));
	serveHtmlFooter();
}

/** Browse Mode. **/
function serveEntryList($blobstore, $fromIdx=0, $size=20, $nameFilter=NULL) {
    global $config;
    
    if (!$blobstore) {
		serve400("Missing Blobstore");
	}
	
	$blobFilepath = $blobstore["filepath"];
	if (!file_exists($blobFilepath)) {
	    serve404("Blobstore not found");
	}
	$blobFile = fopen($blobFilepath, "r");
	if (!$blobFile) {
		serve500();
	}

	$indexOffset = readIndexOffset($blobFile);
	fseek($blobFile, $indexOffset);
	$entries = array();
	$nextFromIdx = $fromIdx + $size;
	for ($lineIdx = 0; $lineIdx < $fromIdx + $size;) {
		$line = fgets($blobFile);
		if (!$line) {
			$nextFromIdx = -1;
			break;
		}
		if ($lineIdx >= $fromIdx) {
			$keyvalues = explode("=", trim($line), 2);
			$name = $keyvalues[0];
			
			if ($nameFilter && strripos($name, $nameFilter) === FALSE) {
				continue;
			}
			
			list($offset, $length, $encoding, $mediaType) = explode(";", $keyvalues[1], 4);
			if (!$mediaType) {
				$mediaType = $config["defaults"]["mediaType"];
			}
			if (!$encoding) {
			    $encoding = $config["defaults"]["encoding"];
			}
			$entries[] = array(
				"name" => $name,
				"offset" => $offset,
				"length" => $length,
				"encoding" => $encoding,
				"mediaType" => $mediaType
			);
		}
		$lineIdx++;
	}
	fclose($blobFile);
    
	serveHtmlHeader();
	render("entrylist", array(
	    "blobstore" => $blobstore, 
	    "entries" => $entries, 
	    "fromIdx" => $fromIdx, 
	    "size" => $size,
	    "nextFromIdx" => $nextFromIdx
	));
	serveHtmlFooter();
}

/** Browse Mode. **/
function browseMain() {
	global $config;
	
	$blobstore = $config["getBlobstore"]();
	if (!$blobstore) {
		serveBlobstoreList();
	} else {
		$browse = getParameter("browse", 1);
		if ($browse == "entrylist") {
			$fromIdx = getParameter("fromIdx", 2);
			if (!$fromIdx) {
				$fromIdx = 0;
			}
			$size = getParameter("size", 3);
			if (!$size) {
				$size = 20;
			}
			$nameFilter = getParameter("nameFilter", 4);
			serveEntryList($blobstore, $fromIdx, $size, $nameFilter);
		} else {
			serveSearchForm($blobstore);
		}
	}
}

?>