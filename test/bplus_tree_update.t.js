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

var on_updated_generator = function on_updated_generator( t, should_be_err, test_string, setup ) {
    
    var on_updated = function on_updated( is_err, sequence_number ) {
    
	t.ok( ! is_err && ! should_be_err || is_err && should_be_err,  test_string ); 
    };

    return on_updated;
};

var on_retrieved_generator = function on_retrieved_generator( t, test_value, test_string ) {
    
    var on_retrieved = function on_retrieved( is_err, value ) {
    
    t.ok( ! is_err && test_value == value,  test_string ); 
    };

    return on_retrieved;
};


var setup = function setup() {

    var file = 'data/bplustree.data';
    if ( path.existsSync( file ) ) {
	fs.unlinkSync( file );
    }

};


test( 'Basic Insert Tests', function (t) {

	setup();

	var bplus_tree = new BPlusTree( 'data/bplustree.data', 4096 );
	t.ok( bplus_tree, 'Successfully constructed BPlusTree object' );

	var on_inserted_first_insert = on_inserted_generator( t, false, 'First insert' );
	bplus_tree.insert( 'foo', 'bar', on_inserted_first_insert );

	var on_first_update = on_updated_generator( t, false, 'Update First Key' );
	bplus_tree.update( 'foo', function( old_value ) { return old_value + 'new'; }, on_first_update );

	var on_inserted_second_key = on_inserted_generator( t, false, 'Insert second key < first' );
	bplus_tree.insert( 'faz', 'baz', on_inserted_second_key );
	
	var on_second_update_internal = on_updated_generator( t, false, 'Update Second key' );
    var on_second_update = function ( err, sequence_number ) {

        on_second_update_internal( err, sequence_number );
        var on_retrieved_first_value = on_retrieved_generator( t, 'barnew', 'Retrieved first updated value' );
        bplus_tree.retrieve( 'foo', on_retrieved_first_value );
    
        var on_retrieved_second_value = on_retrieved_generator( t, 'baznew', 'Retrieved second updated value' );
        bplus_tree.retrieve( 'faz', on_retrieved_second_value );
  
    };
	bplus_tree.update( 'faz', function( old_value ) { return old_value + 'new'; }, on_second_update );

   
    }
    
);



