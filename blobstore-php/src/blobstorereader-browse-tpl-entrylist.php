<?php 
$entries = $viewModel["entries"];
$fromIdx = $viewModel["fromIdx"];
$size = $viewModel["size"];
$nextFromIdx = $viewModel["nextFromIdx"];

$pagination = "";
if ($fromIdx > 0 || $nextFromIdx != -1) {
    $pagination .= "<div style=\"float:right; margin: 10px 0;\">";
    if ($fromIdx > 0) {
        $previousFromIdx = $fromIdx - $size;
        if ($previousFromIdx < 0) {
            $previousFromIdx = 0;
        }
        $pagination .= "<a style=\"margin-left: 8px\" class=\"btn btn-secondary\" role=\"button\" href=\"?blobstore=" . urlencode($viewModel["blobstore"]["id"]) . "&browse=entrylist&fromIdx=$previousFromIdx&size=$size\">Previous</a>";
    }
    if ($nextFromIdx != -1) {
        $pagination .= "<a style=\"margin-left: 8px\" class=\"btn btn-secondary\" role=\"button\" href=\"?blobstore=" . urlencode($viewModel["blobstore"]["id"]) . "&browse=entrylist&fromIdx=$nextFromIdx&size=$size\">Next</a>";
    }
    $pagination .= "</div><br style=\"clear: both\"/>";
}
?>

	<div style="float: left">
		Blobstore: <a href="?blobstore=<?php echo urlencode($viewModel["blobstore"]["id"]) ?>">
			<?php echo htmlspecialchars($viewModel["blobstore"]["label"]) ?>
		</a>
	</div>
	
	<?php echo $pagination; ?>
		
	<table class="table">
		<tr>
			<th>Name</th>
			<th>Stored Size</th>
			<th>Stored Media Type</th>
			<th>Stored Encoding</th>
		</tr>
	<?php foreach ($entries as $entry) { ?>
		<tr>
    		<td>
        		<a href="?blobstore=<?php echo urlencode($viewModel["blobstore"]["id"]); ?>&name=<?php echo urlencode($entry["name"]); ?>">
        			<?php echo htmlspecialchars($entry["name"]); ?>
        		</a>
    		</td>
    		<td>
    			<?php echo htmlspecialchars($entry["length"]); ?>
    		</td>
    		<td>
    			<?php echo htmlspecialchars($entry["mediaType"]); ?>
    		</td>
    		<td>
    			<?php echo htmlspecialchars($entry["encoding"]); ?>
    		</td>
		</tr>
	<?php } ?>

	</table>
	
    <?php echo $pagination; ?>
