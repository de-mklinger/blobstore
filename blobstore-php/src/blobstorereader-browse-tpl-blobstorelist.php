<?php
    
    $files = $viewModel["blobstores"];

	echo "<p>";
	if (count($files) == 1) {
		echo "1 blobstore found.";
	} else {
		echo count($files) . " blobstores found.";
	}
	echo "</p>";
	if (count($files) > 0) {
		echo "<table class=\"table\">";
		echo "<tr>";
		echo "<th>";
		echo "Name";
		echo "</th>";
		echo "<th>";
		echo "Size";
		echo "</th>";
		echo "<th>";
		echo "Date";
		echo "</th>";
		echo "</tr>";
		foreach ($files as $file) {
			echo "<tr>";
			echo "<td>";
			echo "<a href=\"?blobstore=" . htmlspecialchars($file["label"]) . "\">"; 
			echo $file["label"];
			echo "</a>";
			echo "</td>";
			echo "<td>";
			echo getHumanReadableSize($file["size"]);
			echo "</td>";
			echo "<td>";
			echo date("Y-m-d H:i:s (e)", $file["mtime"]);
			echo "</td>";
			echo "</tr>";
		}
		echo "</table>";
	}

?>