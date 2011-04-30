var PageManager = require('../lib/bplus_tree.js').PageManager;

var page_number = process.argv[2];
var page_manager = new PageManager( 'data/bplustree.data', 4096);

page_manager.get_node_at( page_number, function( err, node ) {

	if ( process.argv[3] ) {

	    var key_idx = parseInt(process.argv[3]);
	    console.log( node.keys[key_idx] );
	    var children = [  node.payloads[key_idx],  node.payloads[key_idx+1] ];
	    console.log( children );

	}
	else {
	    console.log(JSON.stringify(node.keys));
	    console.log(JSON.stringify(node.payloads));
	}


	
    } );