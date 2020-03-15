
<script type="text/javascript">
	jQuery(document).ready(function() {
		var $iframe = jQuery("#entry_iframe");
		var $nameInput = jQuery("#name_input");
		jQuery("#showentry_button").click(function(e) {
			if (e) {
				e.preventDefault();
			}
			var name = $nameInput.val();
			if (!name) {
				return;
			}
			var url = "<?php echo $_SERVER['REQUEST_URI']; ?>";
			if (url.indexOf('?') == -1) {
				url = url + "?";
			} else {
				url = url + "&";
			}
			url = url + "name=" + encodeURIComponent(name);
			$iframe.attr("src", url);
			$iframe.show();
		});
		jQuery("#listentries_button").click(function(e) {
			if (e) {
				e.preventDefault();
			}
			var name = $nameInput.val();
			var url = "<?php echo $_SERVER['REQUEST_URI']; ?>";
			if (url.indexOf('?') == -1) {
				url = url + "?";
			} else {
				url = url + "&";
			}
			url = url + "browse=entrylist&nameFilter=" + encodeURIComponent(name);
			window.location.href = url;
		});
	});
</script>
<form>
	<input type="hidden" name="blobstore" value="<?php echo $viewModel["blobstore"]["id"] ?>" />
	<div class="form-group">
		<label>Entry Path:</label>
		<input class="form-control" type="text" name="name" id="name_input" placeholder="Enter blob entry name or prefix filter text"/>
	</div>
	<button class="btn btn-primary" id="showentry_button">Show Entry</button>
	<button class="btn btn-secondary">Open Entry</button>
	<button class="btn btn-secondary" id="listentries_button">List Entries</button>
</form>
<iframe id="entry_iframe" style="display: none; margin-top: 32px; width: 100%; height: 100%; border: 1px solid #ccc; border-radius: 4px; box-shadow: 0 1px 1px rgba(0, 0, 0, 0.075) inset;">
</iframe>
