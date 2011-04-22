/*
   Copyright 2011 Geoff Flarity

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

var fs = require('fs');
var U = require('underscore'); //great utility library

var NODE_TYPES = { 
    ROOT : 0,
    INTERNAL : 1,
    LEAD : 2 
};


/* The PageManager is responsible for reading and writing btree nodes as fs 'Pages'. 
*/
var PageManager = function PageManager( file, page_size ) {
    
    if (! file ) {
	throw "File name is required";
    }
    this.file = file;

    //Keep track of the write offset ourselves
    this.next_page_number = 0; // start at 0
    this.page_size = page_size || 4096; // page size in bytes
    this.write_offset = 0; // next_page_number * page_size

    // TODO read through the file to determine write_offset
    
    //holds an array of zeros, used for convenience when pages padded with zeros
    this.EMPTY_PAGE = new Buffer(4096);
    for (var i = 0; i < 4096; i++ ) {
	this.EMPTY_PAGE[i] = 0;
    }

    //sanity check file, the size should be a multiple of page size
    this.fd = fs.openSync( this.file,  'a+' );

    var stat = fs.statSync( this.file );
    var size = stat.size;
    
    //TODO for now we throw an error, but eventually we'll just go looking for the
    //the last valid root node that was written and truncate after that

    if ( size % this.page_size != 0 ) {
	throw( 'File is corrupt and recovery is not yet implemented' );
    }

    //0 is first page
    this.next_page_number = ( size / this.page_size );
        
};
exports.PageManager = PageManager;


/* Get the page number of last/newest page that was written.
*/
PageManager.prototype.get_last_page_number = function get_last_page_number() {
    return this.next_page_number - 1;
};


/* Get a node at a particular page number.
   callback: on_node_ready( err, node )
*/
PageManager.prototype.get_node_at = function get_node_at( page_number, on_node_ready ) {

    var file_offset = page_number * this.page_size;
    
    var that = this;

    var on_read = function on_read( err, bytesRead, buffer ) {

	if ( err ) {
	    return on_node_ready( err );
	}
	else if ( bytesRead != that.page_size ) {
	    return on_node_ready( 'Could not read the entire page.' );
	}
	else {
	    var node;
	    try { 
		node = Node.from_buffer( buffer );
	    }
	    catch ( err ) {
		return on_node_ready( err );
	    }
	    return on_node_ready( undefined, node );
	}

    };

    var buffer = new Buffer(this.page_size);
    fs.read( this.fd, buffer, 0, this.page_size, file_offset, on_read );

};


/* Get a node at a particular page number, synchronously. This should be used sparingly as it blocks.
*/
PageManager.prototype.get_node_at_sync = function get_node_at_sync( page_number ) {

    var file_offset = page_number * this.page_size;
    var buffer = new Buffer(this.page_size);
    fs.readSync( this.fd, buffer, 0, this.page_size, file_offset );
    var node = Node.from_buffer( buffer );
    return node;
};


/* Write's a serialized node on the next available page and returns the page number.
   callback: on_node_written( err, node, page_number );
*/
PageManager.prototype.write_page = function write_page( page, on_page_written ) {
    
    var that = this;

    var on_written = function on_written( err, written, buffer ) {
	
	if ( written != buffer.length) {
	    return on_page_written( 'Failed to write the entire buffer' );
	}
	else {
	    var page_number = that.next_page_number;
	    that.next_page_number++;
	    return on_page_written( undefined, page, page_number );
	}
    };

    var buffer = new Buffer(this.page_size);
    //padd with 0's
    this.EMPTY_PAGE.copy(buffer, 0, 0 ); 
    buffer.write(page, 0, 'binary');
    fs.write(this.fd, buffer, 0, buffer.length, this.next_page_number * this.page_size, on_written )

};





/* Get file size of file storing the pages.
   callback: on_size_ready( err, size ) 
*/
PageManager.prototype.get_file_size = function get_file_size( on_size_ready ) {

    var on_stats_ready = function on_stats_ready( err, stats ) {
	return on_size_ready( err, stats.size );
    };

    fs.stat( file, on_stats_ready );

};


/* Get file size of file storing the pages.
*/
PageManager.prototype.get_file_size_sync = function get_file_size_sync( on_size_ready ) {
    var stats = fs.statSync( this.file );
    return stats.size;
};


/* BPlusTree is an append only B+Tree and this is it's constructor.
*/
var BPlusTree = function BPlusTree( file, page_size ) {
    //
    if ( ! file ) {
	throw "file is a required param"
    }
    
    this.file = file;
    this.page_size = page_size || 4096;
    this.page_manager = new PageManager( this.file, this.page_size );

    //search backwards for first root node
    var last_page_number = this.page_manager.get_last_page_number();

    if ( last_page_number == -1 ) {
	this.root_page_number = last_page_number; 
    }
    else {
	var page_number = last_page_number;
	while ( page_number > 0 ) { //remember, the smallest tree has a root and a leaf, 2 nodes
	    try { 
		var node = this.page_manager.get_node_at_sync( page_number );
		if ( node.is_root ) {
		    this.root_page_number = page_number;
		    break;
		}
	    }
	    catch ( err ) {
		//some nodes may be corrupt, log for now
		console.log( "Corrupt node found at page number " + page_number + " in file " + this.file + ": " + err  );
	    }
	    page_number--;
	}

    }

    if ( ! this.root_page_number ) { 
	throw "Could not find a single root in this BPlusTree, it's either completely corrupted or not a BPlusTree";
    }
	

};
exports.BPlusTree = BPlusTree;

/* Insert a new key/value into the B+Tree
   callback: on_write_node( err, node, page_number )
*/ 
BPlusTree.prototype.write_node = function write_node( node, on_write_node ) {
    
    var page = node.serialize();
    this.page_manager.write_page( page, on_write_node );

};


/* Insert a new key/value into the B+Tree
   callback: on_inserted( err, sequence_number )
*/ 
BPlusTree.prototype.insert = function insert( key, value, on_inserted ) {
    
    //special case when root node is empty, write the leaf node, then write the root
    if ( this.root_page_number == -1 ) {

	this._insert_first_item(key, value, on_inserted );
    }
    else {

	//setup on our on_root_written call back 
	var on_root_written = function on_root_written( err, node, page_number ) {
	    
	    this.root_page_number = page_number;
	    
	    //TODO figure out sequence number jazz
	    var sequence_number = 0;
	    return on_inserted( err, sequence_number )
	};
	this.insert_append( this.root_page_number, key, value, on_root_written )
    }

};


/* Helper function for inserting the first node into a tree.
   callback: on_inserted( err, sequence_number );
*/
BPlusTree.prototype._insert_first_item = function _insert_first_item( key, value, on_inserted ) {

    var that = this;

    //once we've written a leaf node with the key+value, we'll want to write a root node as well                                                                                                                                          
    var on_leaf_written = function on_leaf_written(err, node, page_number ) {
	
	if ( err ) {
	    return on_inserted( err )
	}
	
	var root = new Node( NODE_TYPES.ROOT, [key], [page_number]  );
	
	//once we've written the root note, call on_inserted callback
	var on_root_written = function on_root_written() {
	    if ( err ) {
		return on_inserted( err )
	    }
	    else {
		//TODO figure out sequence number jazz                                                                                                                                                                                    
		return on_inserted( undefined, 0 );
	    }
	};
	that.write_node( root, on_root_written );
    };

    var leaf = new Node( NODE_TYPES.LEAF, [key], [value] );
    this.write_node( leaf, on_leaf_written );

};



/* Inserts a new key/value pair into sub tree refenced by page_number
   callback: on_inserted( err, page_number, node_to_merge ) 
*/
BPlusTree.prototype.insert_append = function insert_append( page_number, key, value, on_inserted ) {

    var that = this;
    var self = this;
    //reading nodes from the B+Tree is completely asynchronous, here's what we're going to do
    //once the node has been read
    var on_node_read = function on_node_read( err, node ) {

	console.log(self.file);
    
	if (err) {
	    return on_inserted(err);
	}
	
	// recursion end case
	if (node.is_leaf) {
	    //see if the key exists using a binary search
	    var index_of_key = U(node.keys).indexOf( key, true );
	    
	    if ( index_of_key != -1  ) { //key exists, bail
		var err =  'Key already exists!';
		return on_inserted( err );
	    }

	    // create a node from key, value and merge it in
	    // TODO we could juse use this if page_number was null but this is clean and easy for now
	    var node_to_merge = new Node( NODE_TYPES.LEAF, [key], [value] );
	    node.merge( node_to_merge );

	    //use helper to write node, passing either a page_number or a parent node up call stack 
	    //to be merged
	    var on_nodes_written = function on_nodes_written( err, page_number, parent ) {
		return on_inserted( err, page_number, parent );
	    };
	    that._write_nodes_helper( node, on_nodes_written );
	}
	else { 
	    /* The tree looks like: 
	       
	       | keys[1] | keys[2] | .... | key[n] |
	       |    \    |    \       \   |				\
	       |   p[1]  |   p[2]  | .... |  p[n]  | p[n+1] |	

	       That is:
	          | key[i] |
		    /    \
	       | p[i] | p[i+1]

	       Find the greatest i such that: key >= keys[i]
	       
	       if there is such i, we follow payloads[i+1].
	       
	       if there is no such node key, then we follow p[0].
	    */

	    //i defaults to -1, so that payloads[i+1] implies payloads[0];
	    var i = -1;
	    
	    if ( key >= node.keys[0] ) {
		i = 0;
		while ( i < node.payloads.length && key >= node.key[i] ) {
		    i++;
		}
	    }
	    
	    var child_page_number = node.payloads[i+1];
	    
	    /* Almost ready to recurse, but first we setup the call back as it does all the work.
	       This is a bit difficult to visualize, but essentially you're creating a callback
	       chain as you recurse looking for the right leaf node. Eventually you hit the bottom of 
	       the tree, then the callbacks fire all the way back up writing nodes and creating a new 
	       tree.
	    */
	    var on_recursive_insert = function on_recursive_insert(err, page_number, node_to_merge ) {
		if ( err ) {
		    //TODO some kind of error handing/logging here?
		    return on_inserted( err );
		}
		else if ( node_to_merge ) { //a new parent node is bubbling up from below
		    node.merge( node_to_merge );
		}
		else if ( page_number ){ //the child page replacing the one at i + 1 after an append
		    node.payloads[i+1] = page_number;
		} 
		else {
		    return on_inserted( 'Unexpected error has occured' );
		}
		
		if (node.is_root) {
		    //TODO add the new sequence number to the parent's metadata?
		}
		
		//once we write this node, we will either end up with a page_number
		//or a new parent to bubble up
		var on_nodes_written =  function on_nodes_written( err, page_number, node_to_merge  ) {
		    return on_inserted( err, page_number, node_to_merge )
		};
		that._write_nodes_helper( node, on_nodes_written )
		
	    };
	    
	    that.insert_append( child_page_number, key, value, on_recursive_insert );
	    
	}
    };

    //now that we know what we're going to do with that node, we need to get it
    var node;
    if ( ! page_number ) { //create a new empty leaf node , this is the recursive end case
	node = new Node( NODE_TYPES.LEAF );
	return on_node_read( undefined, node );
    }
    else { //slurp node from fs
	return this.page_manager.get_node_at( page_number, on_node_read );
    }

};


/* Write's an internal node to disk. If that node is too big, it splits it up, writes the children
   and returns the parent.
*/
BPlusTree.prototype._write_nodes_helper = function _write_nodes_helper( node, on_written ) {
    
    //Check the size of node
    if ( node.serialize().length > this.page_size ) {
	var components = node.split();
	var parent_key = components[0];
	var left_child = components[1];
	var right_child = components[2];
	
	var parent;
	if( node.is_root() ) {
	    parent = Node( NODE_TYPES.ROOT, [ parent_key ] );
	}
	else {
	    parent = Node( NODE_TYPES.INTERNAL, [ parent_key ] );
	}
	
	var parent = NODE( node.type, [parent_key], [] );
	var errors = [];
	
	//callback for writing node
	var on_node_written = function on_node_written(err, page_number ) {
	    //TODO error handling
	    if ( err ) {
		throw err;
		//errors.push( err )
	    }
	    
	    if ( pages.length == 2 ) {
		//call callback given to .write_nodes_helper since we've added both children now
		//parent is the node_to_merge
		return on_written(undefined, undefined, parent );
	    }

	    //the first two writes add child_page_numbers to parent, the third writes the parent
	    parent.payload.push( page_number );

	}; //end on_node_written

	//write the files and pass the callback we just created
	this.write_node( left_child, on_node_written );
	this.write_node( right_child, on_node_written );
	this.write_node( parent, on_node_written  );
    }
    else { // there was no need to split

	//call back for write_node
	var on_node_written = function on_node_written( err, page_number  ) {
	    //call callback given to .write_nodes_helper way above, we have a page_number if all
	    //went well
	    return on_written( err, page_number, undefined );
	}; // end on_node_written

	//write the files
	this.write_node(node, on_node_written);
    }
    
};

/* Node constructor.
*/
var Node = function Node( type, keys, payloads, metadata ){

    this.type = type;    
    this.is_root = type === NODE_TYPES.ROOT;
    this.is_internal = type === NODE_TYPES.INTERNAL;
    this.is_leaf = type === NODE_TYPES.LEAF;

    this.keys = keys || [];
    this.payloads = payloads || [];

    if ( metadata ) {
	this.metadata = metadata;
    }
};


/* Constructor that takes a serialized json obj representing a node.
*/
Node.from_string = function node_from_string( string ) {

    try {
	var json_obj = JSON.parse( string );
    }
    catch ( err ) {
	console.log(err);
    }

    return new Node( json_obj.type, json_obj.keys, json_obj.payload, json_obj.metadata );
};


/* Constructor that takes a buffer containing serialized json_obj representing a node.
*/
Node.from_buffer = function node_from_buffer( buffer ) {

    
    var buffer_string = buffer.toString();
    var last_index = buffer_string.lastIndexOf('}');
    var object_string = buffer_string.slice( 0, last_index+1 );
    return Node.from_string( object_string );
}


/* Return the serialized representation of this node 
*/
Node.prototype.serialize = function serialize() {

    // TODO this will do for now, but eventually we should make this more compact and optimized
    var json_obj = { type : this.type, keys : this.keys, payloads : this.payloads };

    //some objects contain metadata, but we don't want to include this field in the json obj if it's null anyways
    if  ( this.metadata ) {
	json_obj.metadata = this.metadata;
    }

    return JSON.stringify(json_obj);
};






/* Merge the key + payloads into this leaf node. 
   In the case of an internal node, the payload is the child page number of the next node in the tree.
   In the case of a leaf node, the payload is the actual data being indexed.
*/
Node.prototype.merge = function merge( node_to_merge ) {

    if (node_to_merge.keys.length > 1 ) {
	throw "Node's with more than one key can not currently be merged in";
    }

    //TODO maintain metadata?

    if ( this.type == node_to_merge.type == NODE_TYPES.LEAF ) {
	//find where to insert the key/payload
	var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[0] );
	this.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	this.payloads.splce( insertion_index, 0, node_to_merge.payloads[0] );
    }
    else if ( this.type == node_to_merge.type ) {
	//the insertion_index tells us where the key goes, it also which child
	//gets replaced as it was the one traversed
	//another way of think of it is this child turns into two
	var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[0] );
	this.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	this.payloads[insertion_index] = node_to_merge.payloads[0];
	this.payloads.splice( insertion_index + 1, 0, node_to_merge.payloads[1] );
    }
    else {
	throw "Node's must be the same type in order to merge";
    }
	
    
};


/* Split this node in twain, usually because it's become too big.
 */
Node.prototype.split = function split() {

    //TODO maintain metadata?
    
    //TODO handle the case when there are  only two keys
    //TODO handle the case when the node has only one key
    if( node.keys.length <= 2 ) {
	throw "Splitting nodes with 2 or less keys not currently suported";
    }

    //first find the middle key
    var middle_index = Math.floor(node.keys.length/2);

    //get elements up to and including the middle_index, + 1 accounts for the array starting at 0
    var keys_left = U(node.keys).first( middle_index + 1);
    var payloads_left = U(node.payloads).first( middle_index + 1);

    //get the rest
    var keys_right = U(node.keys).rest( middle_index  + 1);
    var payloads_right = U(node.payloads).rest( middle_index  + 1);
    
    if ( node.is_leaf() ) {
	var child_type = NODE_TYPES.LEAF
	var parent_key = keys_left[-1];
	//now delete the last key from keys right as its now a duplicate, use splice so the array
	//length is one less 
	keys_left.splce(-1, 1 );
    }
    else  {
	var child_type = NODE_TYPES.INTERNAL;
	var parent_key = keys_right[0];
    }

    //create child nodesa
    var child_left = Node( child_type, keys_left, payloads_left );
    var child_right = Node( child_type, keys_right, payloads_right );	
    return [ parent_key, child_left, child_right ]
       
}
