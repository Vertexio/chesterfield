var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var path = require('path');
var fs = require('fs');

//eventually we'll uncomment this once node-tap becomes more mature
//var test = require("tap").test

//fake node-tap
var test = require('./fake-node-tap').test;

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


var number = 10000;

test( 'Insert ' + number + ' keys', function ( t ) {
	
	setup();

	var foo = 'foo';
	var faz = 'faz';

	var bplus_tree = new BPlusTree( 'data/bplustree.data', 4096 );
	t.ok( bplus_tree, 'Successfully constructed BPlusTree object' );
	
	var total_successful = 0, total_failed = 0;
	var on_inserted = function on_inserted( err, sequence_number ) {
	    
	    if ( err ) {
		total_failed++;
	    }
	    else {
		total_successful++;
	    }

	    if ( total_successful + total_failed == number) {
		
		t.ok( total_successful == number, 'Inserted ' + number + ' keys successfully' );
	
		//nested test, bleh, but necessary for now
		//check that dupes can't be inserted
		var total_retrieved_successful = 0, total_retrieved_failed = 0;		

		var on_value_retrieved = function on_value_retrieved( err, value ) {

		    if ( err )  { 
			total_retrieved_failed++ 
		    }
		    else {
			total_retrieved_successful++;
		    }
		    
		    if ( total_retrieved_successful + total_retrieved_failed == number ) {
			t.ok( total_retrieved_successful == number, 'Retrieved ' + number + ' successfully' );
			
			if ( total_retrieved_successful == number ) {

			    //one more nested test
			    var total_dupe_insert_failed = 0, total_dupe_insert_successful = 0;
			    var on_dupe_inserted = function on_dupe_inserted( err, sequence_number ) {

				if ( err )  { 
				    total_dupe_insert_failed++ 
				}
				else {
				    total_dupe_insert_successful++;
				}
				
				if ( total_dupe_insert_successful + total_dupe_insert_failed == number ) {
				    t.ok( total_dupe_insert_failed == number, number + ' dupe inserts should have not succeeded' );

				}

			    };
			    
			    for ( var i = 0; i < number; i++ ) {
				var key;
				if ( i % 2 == 0 ) {
				    key = faz + i;
				}
				else {
				    key = foo + i;
				}

				bplus_tree.insert( key, key, on_dupe_inserted );
			    }

			}

			

		    }
		    
		}; //on_value_retrieved
		
		for ( var i = 0; i < number; i++ ) {
		    var key;
		    if ( i % 2 == 0 ) {
			key = faz + i;
		    }
		    else {
			key = foo + i;
		    }
	
		    bplus_tree.retrieve( key, on_value_retrieved );
		    
		}
		
	    }

	};
	
	for ( var i = 0; i < number; i++ ) {
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