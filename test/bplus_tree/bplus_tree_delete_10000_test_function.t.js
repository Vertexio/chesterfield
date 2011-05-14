var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var path = require('path');
var fs = require('fs');


//eventually we'll uncomment this once node-tap becomes more mature
//var test = require("tap").test

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

var test_function_generator = function test_function_generator( i ) {

    var test = function test( context, continuation ) {
        if ( i % 2 == 0 ) {
    		continuation( null );
		}
		else {
			continuation( 'foo good' );
		}
    };
    return test;
};

var number = 10000;

test( 'Insert ' + number + ' keys with small values', function ( t ) {
	
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
		var total_delete_successful = 0, total_delete_failed = 0;		

		var on_deleted = function on_value_retrieved( err, value ) {

		    if ( err )  { 
			total_delete_failed++ 
		    }
		    else {
			total_delete_successful++;
		    }
		    
		    if ( total_delete_successful + total_delete_failed == number ) {
	
			t.ok( total_delete_successful == number / 2, 'Deleted ' + number / 2 + ' successfully' );
			
			if ( total_delete_successful + total_delete_failed == number ) {
			    
			    var total_retrieved_failed = 0, total_retrieved_successful = 0;
			    var on_retrieved = function on_retrieved( err, value ) {

				if ( err ) {
				    total_retrieved_failed++;
				}
				else {

				    total_retrieved_successful++;
				}
				
				if ( total_retrieved_failed + total_retrieved_successful == number) {
				    
				    t.ok( total_retrieved_failed == number / 2, 'Can only retrieve half' );
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

				bplus_tree.retrieve( key, on_retrieved );
			    }

			}

			

		    }
		    
		}; //on_deleted
		
		for ( var i = 0; i < number; i++ ) {
		    var key;
		    if ( i % 2 == 0 ) {
			key = faz + i;
		    }
		    else {
			key = foo + i;
		    }
            
            var test_function = test_function_generator( i );	
		    bplus_tree.delete( key, on_deleted, test_function );
		    
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