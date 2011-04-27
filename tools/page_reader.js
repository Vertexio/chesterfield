var PageManager = require('../lib/bplus_tree.js').PageManager;

var page_manager = new PageManager( 'data/bplustree.data', 4096);

page_manager.get_node_at( 2002, function( err, node ) {
	console.log(node.keys.join(', '));
    } );