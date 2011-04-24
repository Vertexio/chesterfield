var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var bplus_tree = new BPlusTree( 'data/bplus_tree', 4096 );

//eventually we'll uncomment this once node-tap becomes more mature
//var test = require("tap").test

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

var on_inserted_generator = function on_inserted_generator( t, should_be_err, test_string ) {
    
    var on_inserted = function on_inserted( is_err, sequence_number ) {
	
	t.ok( ! is_err && ! should_be_err || is_err && should_be_err,  test_string ); 
    };

    return on_inserted;
};

test( 'Basic Tests', function (t) {

	var on_inserted_first_insert = on_inserted_generator( t, false, 'First insert' );
	bplus_tree.insert( 'foo', 'bar', on_inserted_first_insert );

	var on_inserted_first_dupe = on_inserted_generator( t, true, 'Insert first duplicate key' );
	bplus_tree.insert( 'foo', 'baz', on_inserted_first_dupe );

	var on_inserted_second_key = on_inserted_generator( t, false, 'Insert second key < first' );
	bplus_tree.insert( 'faz', 'bar', on_inserted_second_key );
	
	var on_inserted_second_dupe = on_inserted_generator( t, true, 'Second duplicate key' );
	bplus_tree.insert( 'foo', 'baz', on_inserted_second_dupe );
	
    }
    
);

