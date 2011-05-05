//fake node-tap
var test = function test( suite_name, on_run_tests ) {

    console.log( 'Running ' + suite_name );

    var t = {
	ok : function( is_ok, test_string ) {
	    if ( is_ok ) {
		console.log( 'ok' + ' - ' + test_string );
	    }
	    else {
		console.log( 'fail' + ' - ' + test_string );
	    }
	}
    }

    on_run_tests( t );
    
};

exports.test = test;