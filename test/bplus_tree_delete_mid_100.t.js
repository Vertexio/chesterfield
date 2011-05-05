var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var path = require('path');
var fs = require('fs');

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

var number = 100;

test( 'Insert keys with medium sized value', function( t ) {
	
	setup();

	var foo = 'foo';
	var faz = 'faz';


	function generate_value( key ) {
	    var base_data = [ 1, 2, 3, 4, 5, 6, 7, 8, 'nine', 'ten' ];
	    var base_data_nested =  [base_data, base_data, base_data, base_data, base_data, 
				 base_data, base_data, base_data, base_data, base_data];

	    //~3140 characters when serialized
	    var base_value = { data:  [ base_data_nested, base_data_nested, base_data_nested, base_data_nested, 
					base_data_nested, base_data_nested, base_data_nested, base_data_nested, 
					base_data_nested, base_data_nested ] };	    
	    
	    base_value._id = key;
	    return base_value;

	}

	
	var bplus_tree = new BPlusTree( 'data/bplustree.data', 4096 );

	var total_insert_successful = 0, total_insert_failed = 0;
	var on_inserted = function on_inserted( err, sequence_number ) {
	    
	    if ( err ) {
		total_insert_failed++;
	    }
	    else {
		total_insert_successful++;
	    }


	    if ( total_insert_successful + total_insert_failed == number ) {

		t.ok( total_insert_successful == number, 'Inserted ' + number + ' successfully' );

		
		var total_delete_successful = 0, total_delete_failed = 0;
		
		var on_deleted = function on_deleted( err, value ) {
		    
		    if ( err )  { 
			total_delete_failed++ 
		    }
		    else {
			total_delete_successful++;
		    }
		    
		    if ( total_delete_successful + total_delete_failed == number ) {
			
			t.ok( total_delete_successful == number, 'Deleted ' + number + ' successfully' );
			
			var total_retrieved_successful = 0, total_retrieved_failed = 0;	       
			var on_value_retrieved = function on_value_retrieved( err, value ) {
			    
			    if ( err )  { 
				total_retrieved_failed++ 
			    }
			    else {
				total_retrieved_successful++;
			    }
			    
			    if ( total_retrieved_successful + total_retrieved_failed == number ) {
				t.ok( total_retrieved_failed == number, 'Shouldn\'t be able retrieve any of the ' + number + ' keys successfully' );
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
		    
		    bplus_tree.delete( key, on_deleted );
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
	    
	    var value = generate_value( key );
	    bplus_tree.insert( key, value, on_inserted );
	}
	
    } );