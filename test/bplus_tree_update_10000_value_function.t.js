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

var value_function_generator = function value_function_generator(i, value) {
        var value_function = function value_function(context, continuation) {
                if (i % 2 == 0) {
                    continuation('faz bad');
                }
                else {
                    continuation(null, value); //foo good   
                }
            };
        return value_function;
};

var setup = function setup() {

    var file = 'data/bplustree.data';
    if ( path.existsSync( file ) ) {
	fs.unlinkSync( file );
    }

};


var number = 10000;

test('Insert ' + number + ' keys with small values', function(t) {

    setup();

    var foo = 'foo';
    var faz = 'faz';

    var bplus_tree = new BPlusTree('data/bplustree.data', 4096);
    t.ok(bplus_tree, 'Successfully constructed BPlusTree object');

    var total_successful = 0,
        total_failed = 0;
    var on_inserted = function on_inserted(err, sequence_number) {

            if (err) {
                total_failed++;
            }
            else {
                total_successful++;
            }

            if (total_successful + total_failed == number) {

                t.ok(total_successful == number, 'Inserted ' + number + ' keys successfully');

                var total_updated_successfully = 0,
                    total_updated_failed = 0;
                var on_updated = function on_updated(err, sequence_number) {

                        if (err) {
                            total_updated_failed++;
                        }
                        else {

                            total_updated_successfully++;
                        }

                        if (total_updated_failed + total_updated_successfully == number) {

                            t.ok(total_updated_successfully == number / 2, 'Updated ' + number / 2 + ' keys successfully');

                            //nested test, bleh, but necessary for now
                            var total_retrieved_successful = 0,
                                total_retrieved_failed = 0;

                            var emitted = 0;

                            var on_walk_complete = function on_walk_complete(err, value) {
                                    t.ok(emitted == number / 2, 'Walk emitted updated values ' + number / 2 + ' times');

                                }; //on_walk_complete
                            var emit = function(key, value) {

                                    if (value == key + 'new') {
                                        emitted++;
                                    }
                                }
                            bplus_tree.walk(emit, on_walk_complete);

                        }

                    };
                for (var i = 0; i < number; i++) {
                    var key;
                    if (i % 2 == 0) {
                        key = faz + i;
                    }
                    else {
                        key = foo + i;
                    }
                    var value = key + 'new';
                    var value_function = value_function_generator( i, value );
                    bplus_tree.update(key, value_function, on_updated);
                }

            }

        };

    for (var i = 0; i < number; i++) {
        var key;
        if (i % 2 == 0) {
            key = faz + i;
        }
        else {
            key = foo + i;
        }

        bplus_tree.insert(key, key, on_inserted);

    }



});