var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var path = require('path');
var fs = require('fs');

var test = require('./fake-node-tap').test;

var number = 1000;

var errors_encountered = 0;
var on_value_retrieved_generator = function on_value_retrieved_generator(i, t) {

    var on_value_retrieved = function on_value_retrieved(err, value ) {
	
	if (i > 321 ) {
	    if ( err ) {
		errors_encountered++;
	    }

	    if (i == number - 1) {
		
		t.ok( errors_encountered == 0, 'Successfully retrieved foo321 every time with no errors' );

	    }
	    
	}
    };

    return on_value_retrieved;
};

var on_inserted_generator = function on_inserted_generator( i, bplus_tree, t ) {

    var on_inserted = function on_inserted( err, sequence_number ) {
	
	    if ( err ) {
		errors_encountered++;
	    }

	    
	    var on_value_retrieved = on_value_retrieved_generator( i, t )
	    bplus_tree.retrieve( 'foo321', on_value_retrieved );

    };

    return on_inserted;
    
};


var setup = function setup() {

    var file = 'data/bplustree.data';
    if ( path.existsSync( file ) ) {
	fs.unlinkSync( file );
    }

};



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
	
	for ( var i = 0; i < number; i++ ) {
	    var key;
	    if ( i % 2 == 0 ) {
		key = faz + i;
	    }
	    else {
		key = foo + i;
	    }

	    var value = generate_value( key );
	    var on_inserted = on_inserted_generator( i, bplus_tree, t )
	    bplus_tree.insert( key, value, on_inserted );
	}

    } );

