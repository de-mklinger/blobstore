<?php

function resolveLocalDirectory() {
    if (strstr(__FILE__, "phar://") == 0) {
        $_default_config_path = substr(__FILE__, strlen("phar://"));
        $_innerIdx = strpos($_default_config_path, ".phar/");
        if ($_innerIdx !== FALSE) {
            return dirname(substr($_default_config_path, 0, $_innerIdx));
        }
    } else {
        return dirname(__FILE__);
    }
}

function isValidBlobstoreName($blobstoreName) {
    return $blobstoreName
    && strstr($blobstoreName, "..") === FALSE
    && strstr($blobstoreName, "/") === FALSE
    && strstr($blobstoreName, "\\") === FALSE;
}

function getBlobstoreObject($blobstoreName, $blobstorePath) {
    $entry = stat($blobstorePath);
    $entry["label"] = $blobstoreName;
    $entry["id"] = $blobstoreName;
    $entry["filepath"] = $blobstorePath;
    return $entry;
}

function getBestSupportedMimeType($mimeTypes = null) {
    if (!$_SERVER || !array_key_exists("HTTP_ACCEPT", $_SERVER)) {
        return null;
    }
    
	// Values will be stored in this array
	$AcceptTypes = Array ();

	// Accept header is case insensitive, and whitespace isn’t important
	$accept = strtolower(str_replace(' ', '', $_SERVER['HTTP_ACCEPT']));
	// divide it into parts in the place of a ","
	$accept = explode(',', $accept);
	foreach ($accept as $a) {
		// the default quality is 1.
		$q = 1;
		// check if there is a different quality
		if (strpos($a, ';q=')) {
			// divide "mime/type;q=X" into two parts: "mime/type" i "X"
			list($a, $q) = explode(';q=', $a);
		}
		// mime-type $a is accepted with the quality $q
		// WARNING: $q == 0 means, that mime-type isn’t supported!
		$AcceptTypes[$a] = $q;
	}
	arsort($AcceptTypes);

	// if no parameter was passed, just return parsed data
	if (!$mimeTypes) return $AcceptTypes;

	$mimeTypes = array_map('strtolower', (array)$mimeTypes);

	// let’s check our supported types:
	foreach ($AcceptTypes as $mime => $q) {
		if ($q && in_array($mime, $mimeTypes)) return $mime;
	}
	// no mime-type found
	return null;
}

function isGzipSupported() {
	// Accept header is case insensitive, and whitespace isn’t important
	$acceptEncoding = strtolower(str_replace(' ', '', $_SERVER['HTTP_ACCEPT_ENCODING']));
	// divide it into parts in the place of a ","
	$acceptEncoding = explode(',', $acceptEncoding);
	foreach ($acceptEncoding as $ae) {
		if ("gzip" == $ae) {
			return true;
		}
	}
	return false;
}

function serve404($text = "") {
	serveError(404, "Not Found", $text);
}

function serve400($text = "") {
    serveError(400, "Bad Request", $text);
}

function serve500($text = "") {
	serveError(500, "Internal Server Error", $text);
}

function serveError($code, $msg, $text) {
	if (php_sapi_name() == "cli") {
	    global $argv;
	    fwrite(STDERR, "Error: " . $text . PHP_EOL);
	    fwrite(STDERR, "Usage: " . $argv[0] . " <blob-name> <entry-name>" . PHP_EOL);
	    exit;
	} else {
	    $protocol = (isset($_SERVER['SERVER_PROTOCOL']) ? $_SERVER['SERVER_PROTOCOL'] : 'HTTP/1.0');
	    header($protocol . ' ' . $code . ' ' . $msg);
	    header("Content-Type: text/plain");
	    if (!$text) {
	        $text = $msg;
	    }
	    echo "Error: $text\n";
	    exit(1);
	}
}

function getEntryName() {
	// no check needed here, searching within the blob file should be
	// safe for all inputs.
	return getParameter("name", 1, 255);
}

function getParameter($name, $idx, $length = 1) {
	global $_SERVER, $_REQUEST, $argv;
	if ($_SERVER && isset($_SERVER['PATH_INFO']) && $_SERVER['PATH_INFO']) {
		$pathInfo = $_SERVER['PATH_INFO'];
		if ($pathInfo && strlen($pathInfo) > 1 && $pathInfo[0] == "/") {
			$parts = explode("/", substr($pathInfo, 1));
			if (count($parts) > $idx && $parts[$idx]) {
				$path = "";
				for ($i = $idx; $i < $idx + $length && $i < count($parts); $i++) {
					if (strlen($path) > 0) {
						$path .= "/";
					}
					$path .= $parts[$i];
				}
				return $path;
			}
		}
	}
	if ($name && $_REQUEST && isset($_REQUEST[$name]) && $_REQUEST[$name]) {
		return $_REQUEST[$name];
	}
	if ($argv && count($argv) > $idx + 1 && $argv[$idx + 1]) {
		return $argv[$idx + 1];
	}
}

?>