var BPlusTree = require('../lib/bplus_tree').BPlusTree;

var bplus_tree = new BPlusTree( 'data/bplus_tree', 4096 );

var on_inserted = function on_inserted( err, sequence_number ) {
    if ( err ) {
	console.log( err );
    }
    else {
	console.log('ok');
    }
	
};
//ok
bplus_tree.insert( 'foo', 'bar', on_inserted );
//err
bplus_tree.insert( 'foo', 'baz', on_inserted );
//ok
bplus_tree.insert( 'faz', 'bar', on_inserted );

//err
bplus_tree.insert( 'foo', 'bin', on_inserted );
//err
bplus_tree.insert( 'foo', 'bak', on_inserted );