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

var number = 2;

test( 'Insert keys with medium sized value', function( t ) {
	
	setup();

	var foo = 'foo';
	var faz = 'faz';

	var base_data = [ 1, 2, 3, 4, 5, 6, 7, 8, 'nine', 'ten' ];
	var base_data_nested =  [base_data, base_data, base_data, base_data, base_data, 
				 base_data, base_data, base_data, base_data, base_data];

	//~3140 characters when serialized
	var base_value = { data:  [ base_data_nested, base_data_nested, base_data_nested, base_data_nested, 
					   base_data_nested, base_data_nested, base_data_nested, base_data_nested, 
				    base_data_nested, base_data_nested ] };


	
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
	    }

	};



	base_value._id = foo;
	bplus_tree.insert( foo, base_value, on_inserted );

	base_value._id = faz;
	bplus_tree.insert( faz, base_value, on_inserted );


    } );