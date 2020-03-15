<?php

/**
 * Blobstore Reader and Browser Configuration.
 * 
 * Place a file `config.php` next to this file or next to the .phar
 * file to extend configuration.
 */

$config = array(

	/**
	 * The base directory for blob files on the local filesystem.
	 */
	"blobFilesBaseDir" => resolveLocalDirectory(),

    /**
     * Default blob store name (without ".blob" extension).
     */
    "defaultBlobstoreName" => FALSE,
    
    /**
     * Set to TRUE to enable auto index in blobFilesBaseDir.
     */
    "autoIndex" => FALSE,
    
    /**
     * Blob store defaults.
     */
    "defaults" => array(
        "encoding" => "gzip",
        "mediaType" => "application/json"
    ),

    /**
     * Blob store lookup function.
     */
    "getBlobstore" => function() {
        global $config;

        $blobstorePath = NULL;
        
        $blobstoreName = getParameter("blobstore", 0);
        
        if (!$blobstoreName) {
            $blobstoreName = $config["defaultBlobstoreName"];
        }
        
        if (isValidBlobstoreName($blobstoreName)) {
            $blobstorePath = $config["blobstoresBaseDir"] . "/" . $blobstoreName . ".blob";
        }
        
        if ($blobstorePath && file_exists($blobstorePath)) {
            return getBlobstoreObject($blobstoreName, $blobstorePath);
        } else {
            return NULL;
        }
    },
    
    /**
     * Blob store listing function.
     */
    "listBlobstores" => function() {
        global $config;
        
        if ($config["autoIndex"] === TRUE) {
            
        	$handle = opendir($config["blobstoresBaseDir"]);
        	if (!$handle) {
        		return array();
        	}
        
        	$blobstores = array();
        	while (false !== ($file = readdir($handle))) {
        	    if (substr($file, -strlen(".blob")) === ".blob") { // file.endsWith(".blob")
            	    $blobstoreName = basename($file, ".blob");
            	    if (isValidBlobstoreName($blobstoreName)) {
            		    $blobstorePath = $config["blobstoresBaseDir"] . "/" . $blobstoreName . ".blob";
            		    if (file_exists($blobstorePath)) {
            		        $blobstores[] = getBlobstoreObject($blobstoreName, $blobstorePath);
            			}
            		}
        	    }
        	}
        	closedir($handle);
        	
        	return $blobstores;
            
        } else {
            $blobstore = $config["getBlobstore"]();
            if ($blobstore) {
                return array($blobstore);
            } else {
                return array();
            }
        }
    },

);

$_extra_config_path = resolveLocalDirectory() . "/config.php";
if ($_extra_config_path && file_exists($_extra_config_path)) {
    include($_extra_config_path);
}

?>