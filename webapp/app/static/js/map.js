// var mapOptions = {
//     center: new google.maps.LatLng(37.7831,-122.4039),
//     zoom: 12,
//     mapTypeId: google.maps.MapTypeId.ROADMAP
// };

//map options
var mapOptions = {
	zoom: 5,
	center: new google.maps.LatLng(37.09024, -100.712891),
	panControl: true,
	panControlOptions: {
		position: google.maps.ControlPosition.BOTTOM_LEFT
	},
	zoomControl: true,
	zoomControlOptions: {
		style: google.maps.ZoomControlStyle.LARGE,
		position: google.maps.ControlPosition.RIGHT_CENTER
	},
	scaleControl: false
};
var mapobj = new google.maps.Map(document.getElementById('map'), mapOptions);

  
// //map marker
// var markerOptions = {
//     position: new google.maps.LatLng(37.7831, -122.4039)
// };
// var marker = new google.maps.Marker(markerOptions);
// marker.setMap(mapobj);


// $(function field_lookup () {
// 	$("#fieldSearch").change(function()
// 	});
// });



// $(function() {
// 	var submit_form = function(e) {
// 	  	$.getJSON($SCRIPT_ROOT + '/wellsearch', {sub: $('input[name="sub"]').val(),}, 
// 		  	function(data) {
// 		  		var obj = $('#result').text(data.result);
// 				obj.html(obj.html().replace(/\n/g,'<br/>'));
// 				var tab ='<span style="display:inline-block; width: 10;"></span>'
// 				obj.html(obj.html().replace(/\n/g,'<br/>'));
// 				obj.html(obj.html().replace(/\t/g,tab));
// 		  	}
// 	  	);
// 	return false;
// 	};

// $('input[type=text]').bind('keyup', function(e) {
//     submit_form(e);
//     });





