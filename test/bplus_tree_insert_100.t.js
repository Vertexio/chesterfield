var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var path = require('path');
var fs = require('fs');

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

var on_inserted_generator = function on_inserted_generator( t, should_be_err, test_string, setup ) {
    
    var on_inserted = function on_inserted( is_err, sequence_number ) {
	
	t.ok( ! is_err && ! should_be_err || is_err && should_be_err,  test_string ); 
    };

    return on_inserted;
};

var setup = function setup() {

    var file = 'data/bplustree.data';
    if ( path.existsSync( file ) ) {
	fs.unlinkSync( file );
    }

};


test( 'Insert 100 keys', function ( t ) {
	
	setup();

	var foo = 'foo';
	var faz = 'faz';

	var bplus_tree = new BPlusTree( 'data/bplustree.data', 4096 );
	t.ok( bplus_tree, 'Successfully constructed BPlusTree object' );
	
	var total_successful = 0, total_failed = 0;
	var on_inserted = function on_inserted( err, sequence_number ) {
	    
	    if ( err ) {
		total_failed++
	    } 
	    else { 
		total_successful++;
	    };

	    if ( total_successful + total_failed == 100 ) {
		
		t.ok( total_successful == 100, 'Inserted 100 keys successfully' );
		
		//nested test, bleh, but necessary for now
		//check that dupes can't be inserted
		var total_nested_successful = 0, total_nested_failed = 0;		

		var on_nested_inserted = function on_nested_inserted( err, sequence_number ) {

		    if ( err )  { 
			total_nested_failed++;
		    }
		    else {
			total_nested_successful++;
		    }
		    
		    if ( total_nested_successful + total_nested_failed == 100 ) {
			t.ok( total_nested_failed == 100, 'Could not insert all 100 dupes' );
		    }
		    
		}; //on_nested_inserted
		
		for ( var i = 0; i < 100; i++ ) {
		    var key;
		    if ( i % 2 == 0 ) {
			key = faz + i;
		    }
		    else {
			key = foo + i;
		    }
	
		    bplus_tree.insert( key, key, on_nested_inserted );
		    
		}
		

	    }

	};
	
	for ( var i = 0; i < 100; i++ ) {
	    var key;
	    if ( i % 2 == 0 ) {
		key = faz + i;
	    }
	    else {
		key = foo + i;
	    }
	
	    bplus_tree.insert( key, key, on_inserted );
	
	}
	
    });