/*
   Copyright 2011 Geoffrey Flarity

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
var U = require('undersore');

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
    this.EMPTY_PAGE = [];
    for (var i = 0; i < 4096; i++ ) {
	EMPTY_PAGE.push(0);
    }


};
exports.PageManager = PageManager;

/* Get the page number of last/newest page 
*/
PageManager.prototype.get_last_page_number = function get_last_page_number() {
    return this.next_page_number - 1;
};

/* Get a node at a particular page number
*/
PageManager.prototype.get_node_at = function get_node_at( page_number ) {

};

/* Write's a serialized node on the next available page and returns the page number.
*/
PageManager.prototype.write_node = function write_node( node ) {

}


    var BPlusTree = function BPlusTree( file, page_size ) {
    //
    if ( ! file ) {
	throw "file is a required param"
    }
    
    this.page_size = page_size || 4096;

    //TODO create a root node
    //TODO create a page manager

    
 
}
exports.BPlusTree = BPlusTree;

/* Insert a new key/value into the B+Tree
   on_inserted( err, sequence_number )
*/ 
BPlustree.prototype.insert = function insert( key, value, on_inserted ) {

     //TODO look up root page_number
    //TODO call insert_append on it

    
}

/* Inserts a new key/value pair into sub tree refenced by page_number
   on_inserted( err, page_number, node_to_merge ) 
*/
BPlusTree.prototype.insert_append = function insert_append( page_number, key, value, on_inserted ) {

    var node;
    if ( ! page_number ) { // need to create a new empty leaf node 
	node = new Node( NODE_TYPES.LEAF );
    }
    else { //slurp node from fs
	node = this.page_manager.get_node_at( page_number );
    }

    // recursion end case
    if (node.is_leaf) {

	//TODO check if key already exists

	// create a node from key, value and merge it in
	// TODO we could juse use this if page_number was null but this is clean and easy for now
	var node_to_merge = Node( NODE_TYPES.LEAF, [key], [value] );
	node.merge( node_to_merge );

	//use helper to write node, passing either a page_number or a parent node up call stack 
	//to be merged
	var on_written = function on_written( err, page_number, parent ) {
	    return on_inserted( err, page_number, parent );
	};
	this._write_node_helper( node, on_written );
    
    else { 
	/* The tree looks like: 
       
           | keys[1] | keys[2] | .... | key[n] |
               |    \    |    \  .... \   |   \
           |   p[1]  |   p[2]  | .... |  p[n]  | p[n+1] |	

	   That is:

	          | key[i] |
                     / \
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
	   chain as you recurse looking for a leaf node. Eventually you hit the bottom of the tree,
	   then you fire callbacks all the way back up writing nodes and creating a new tree.
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
		node.payload[i+1] = page_number;
	    } 
	    else {
		return on_inserted( 'Unexpected error has occured' );
	    }

	    if (node.is_root) {
		//TODO add the new sequence number to the parent's metadata?
	    }
	
	    //once we write this node, we will either end up with a page_number
	    //ore a new parent to bubble up
	    var on_written =  function on_written( err, page_number, parent  ) {
		return on_inserted( err, page_number, parent )
	    };
	    this._write_node_helper( node, on_written )
	    
	};

	insert_append( child_page_number, key, value, on_recursive_insert );
	
    }
	
    }
};


/* Write's an internal node to disk. If that node is too big, it splits it up, writes the children
   and returns the parent.
*/
BPlusTree.prototype._write_node_helper = function _write_node_helper( node, on_written ) {

    //Check the size of node
    if ( node.serialize().length > this.page_size ) {
	var components = node.split();
	var parent_key = components[0];
	var left_child = components[1];
	var right_child = components[2];
	
	var parent;
	if( node.type == NODE_TYPES.ROOT ) {
	    parent = Node( NODE_TYPES.ROOT, [ parent_key ], [] );
	}
	else {
	    parent = Node( NODE_TYPES.INTERNAL, [ parent_key ], [] );
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
		//call callback given to .write_node_helper
		return on_written(undefined, undefined, parent );
	    }

	    //the first two writes add child_page_numbers to parent, the third writes it
	    //TODO kind of messy but it'll do for now
	    parent.payload.push( page_number );

	}; //end on_node_written

	//write the files and pass the callback we just created
	this.page_manager.write( left_child, on_node_written );
	this.page_manager.write( right_child, on_node_written );
	this.page_manager.write( parent, on_node_written  );
    }
    else {
	//call back for writing node
	var on_node_written = function on_node_written( err, page_number  ) {
	    //call callback given to .write_node_helper
	    return on_written( err, page_number, undefined );
	}; // end on_node_written

	//write the files
	this.page_manager.write(node, on_node_written);
    }
    
};

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
    var json_obj = JSON.parse( string );
    return new Node( json_obj.type, json_obj.keys, json_obj.payload, json_obj.metadata );
};


/* Constructor that takes a buffer containing serialized json_obj representing a node.
*/
Node.from_buffer = function node_from_buffer( buffer ) {
    // TODO maybe there's a better way?
    return Node.from_string( buffer.toString() );
}


/* Return the serialized representation of this node 
*/
Node.prototype.serialize = function serialize() {

    // TODO this will do for now, but eventually we should make this more compact and optimized
    var json_obj = { type : this.type, keys : this.keys, payloads : this.payloads }:

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
Node.prototype.merge = function merge( node_to_merge, replace) {

    //TODO maintain metadata?

    if (node_to_merge.keys.length > 1 ) {
	throw "Node's with more than one key are not currently supported";
    }
    if ( node.type == node_to_merge.type == NODE_TYPES.LEAF ) {
	    var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[i] );
	    node.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	    node.payload.splce( insertion_index, 0, node_to_merge.payload[0] );
    }
    else {
	var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[i] );
	node.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	//replace with left most child
	node.payload[i] = node.payload[0];
	node.payload.splice( insertion_index + 1, 0, node.payload[1] );
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
    
    if ( node.type == NODE_TYPES.LEAF ) {
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