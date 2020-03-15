<?php

header_remove("X-Powered-By");
header("Vary: Accept-Encoding, Accept");

if (isBrowseMode()) {
	browseMain();
} else {
	blobStoreReaderMain();
}

?>