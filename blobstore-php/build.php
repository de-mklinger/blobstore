#!/usr/bin/php -dphar.readonly=0
<?php 

$pharName = "target/blobstorereader.phar";

if (!file_exists(dirname($pharName))) {
    mkdir(dirname($pharName));
}

if (file_exists($pharName)) {
    unlink($pharName);
}

{
    $phar = new Phar($pharName);
    $phar->setStub($phar->createDefaultStub('blobstorereader.php', 'blobstorereader.php'));
    $phar->buildFromDirectory("src", '/^.*\.php$/');
}

{
    $phar2 = new Phar($pharName);
    foreach (new RecursiveIteratorIterator($phar2) as $file) {
        echo $file . "\n";
    }
}

?>