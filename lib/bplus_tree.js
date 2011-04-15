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


var BPlusTree = function BPlusTree( file ) {
    //
    if ( ! file ) {
	throw "file is a required param"
    }

    //create a root node

    
 
}
exports.BPlusTree = BPlusTree;

BPlusTree.prototype.insert_append = function insert_append( child_page, key, value ) {

    var node;
    //if child_page is null, time for a new leaf

    //else slurp node from fs

    // recursion end case
    if (node.is_leaf) {

	//merge key, value into node

	//if it is too big now
	    //split it

	    //get the left most key from the second node
	    //this the left most key in the entire sub tree

	    //write append each node to tree

	    //return [ first_page_number, left_most_key, second_page_number ]
	//else we're good
	    //write append node to ree
            //return [ new_page_name ]
	
    }
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

	   if there is such i, we follow p[i+1].

	   if there is no such node key, then we follow p[0].

	   now we recurse
	   var results = insert_append( next_child_page, key, value );
	
	   if results contains only one value, this is the child page replacing the one at i + 1
	   otherwise we should have:
	   [ first_child_page, second_child_page, smallest_key_of_second_child_page ]

	   think of it like this:

              smallest_key_second_child_age
	             |                 \
              first_child_page    second_child_page

	   Everything in the key array at positions > i get shifted by 1 and 
	   smallest_key_second_child_age is inserted at i + 1
	   
	   Similarily, everything at position > i + 1 get's shifted by 1 in the child page array 
	   and element i + 1 is replaced with first_child_page, element i+2 is replaced by
	   second_child_page. 

	   A new well formed, subtree was just inserted into it's new home. 

	*/    
	
	//Check the size of our new node. 

	//if it's too big, we have to split it in half, create a new parent and send it up the stack
	//TODO how big is too big?
	    if (node.is_root) {
		//TODO clarify in terms of i like above
		//pick middle key as new root key
		//everything twrite the halves as an internal nodes
		//write root pointing to internals nodes
		//return [ root_page_number ]
	    }
	    else { 
		//TODO can't we do these the same? ie, remove the left most key from second node?
		//node.is_intertal
		//split in half
		//write haves as interntal nodes
		//return [ first_page_number, left_most_key, second_page_number ]
		
	    }

	//else not too many keys
	    if (node.is_root) {
		//write root node	    
	    }
	    else {
		//node.is_internal
		//write page as internal node
	    }
	    // return [ new_page ]	              	

    }
    
};

var Node = function Node( type, keys, payload, metadata ){

    this.type = type;    
    this.is_root = type === NODE_TYPES.ROOT;
    this.is_internal = type === NODE_TYPES.INTERNAL;
    this.is_leaf = type === NODE_TYPES.LEAF;

    this.keys = keys;
    this.payload = payload;

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
    var json_obj = { type : this.type, keys : this.keys, payload : this.payload }:

    //some objects contain metadata, but we don't want to include this field in the json obj if it's null anyways
    if  ( this.metadata ) {
	json_obj.metadata = this.metadata;
    }

    return JSON.stringify(json_obj);
};

/* Merge the key/payload into this leaf node. 
   In the case of an internal node, the payload is the child page number of the next node in the tree.
   In the case of a leaf node, the payload is the actual data being indexed.
*/
Node.prototype.merge_in = function merge_in( key, payload_left, payload_right ) {
    // TODO the origin needs to be considered when merging into an internal node...
    // this should perhaps stay part of insert_append
};

/* Split this node in twain, usually because it's become too big.
 */
Node.prototype.split = function split() {

}