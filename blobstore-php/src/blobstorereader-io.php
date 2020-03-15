<?php

function searchBlobEntry($f, $searchName) {
	$indexOffset = readIndexOffset($f);
	// because we read the second line after each seek there is no way the
	// binary search will find the first line, so check it first.
	fseek($f, $indexOffset);
	$line = fgets($f);
	if (!$line) {
		return null;
	}
	$keyvalues = explode("=", trim($line), 2);
	$key = $keyvalues[0];
	if ($key == $searchName) {
		return explode(";", $keyvalues[1], 4);
	}

	// set up the binary search.
	$beg = $indexOffset;
	$fstat = fstat($f);
	$end = $fstat['size'];
	while ($beg < $end) {
		$mid = floor($beg + ($end - $beg) / 2);
		//echo "beg: $beg / end: $end / mid: $mid\n";
		fseek($f, $mid);
		fgets($f);
		$line = fgets($f);
		if (!$line) {
			// end of file, look before
			$end = $mid - 1;
		} else {
			$keyvalues = explode("=", trim($line), 2);
			$key = $keyvalues[0];
			$n = strcmp($key, $searchName);
			if ($n > 0) {
					// what we found is greater than the target, so look
					// before it.
					$end = $mid - 1;
			} else if ($n < 0) {
					// otherwise, look after it.
					$beg = $mid + 1;
			} else {
					return explode(";", $keyvalues[1], 4);
			}
		}
	}

	// The search falls through when the range is narrowed to nothing.
	fseek($f, $beg);
	fgets($f);
	$line = fgets($f);
	if (!$line) {
		return null;
	}
	$keyvalues = explode("=", trim($line), 2);
	$key = $keyvalues[0];
	if ($key == $searchName) {
		return explode(";", $keyvalues[1], 4);
	}
	return null;
}

function readIndexOffset($f) {
	fseek($f, 0);
	$line = fgets($f);
	if (!$line) {
		serve500("Invalid blob file (1)");
	}
	// header is 32 bytes including newline
	if (strlen($line) != 32) {
		serve500("Invalid blob file (2)");
	}
	if (!preg_match("/^indexOffset=0*(\d+?)$/", $line, $m)) {
		serve500("Invalid blob file (3)");
	}
	$offset = $m[1];
	return $offset;
}

?>