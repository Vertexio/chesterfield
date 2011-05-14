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

var on_deleted_generator = function on_deleted_generator( t, should_be_err, test_string ) {
    
    var on_deleted = function on_deleted( is_err ) {

	t.ok( ! is_err && ! should_be_err || is_err && should_be_err,  test_string ); 

    };

    return on_deleted;
};

var setup = function setup() {

    var file = 'data/bplustree.data';
    if ( path.existsSync( file ) ) {
	fs.unlinkSync( file );
    }

};


test( 'Basic Delete Tests', function (t) {

	setup();

	var bplus_tree = new BPlusTree( 'data/bplustree.data', 4096 );
	t.ok( bplus_tree, 'Successfully constructed BPlusTree object' );

	var on_inserted_first_insert = on_inserted_generator( t, false, 'First insert' );
	bplus_tree.insert( 'foo', 'bar', on_inserted_first_insert );

	var on_deleted_first_key = on_deleted_generator( t, false, 'Delete first and only key' );
	bplus_tree.delete( 'foo', on_deleted_first_key );

	var on_inserted_first_insert_again = on_inserted_generator( t, false, 'First insert again to replace deletion' );
	bplus_tree.insert( 'foo', 'bar', on_inserted_first_insert_again );

	var on_inserted_second_key = on_inserted_generator( t, false, 'Insert second key < first' );
	bplus_tree.insert( 'faz', 'bar', on_inserted_second_key );

	var on_deleted_first_again = on_deleted_generator( t, false, 'Delete first key' );
	bplus_tree.delete( 'foo', on_deleted_first_again );

	var on_deleted_second_key = on_deleted_generator( t, false, 'Delete second key' );
	bplus_tree.delete( 'faz', on_deleted_second_key );

    }
    
);



