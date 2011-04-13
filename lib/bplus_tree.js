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


var BPlusTree = function BPlusTree() {

}
exports.BPlusTree = BPlusTree;

BPlusTree.prototype.insert_append = function insert_append( child_page, key, value ) {

    var node;
    //TODO can child_page ever be null?

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
	// pick a child and recurse
	var results = insert_append( next_child_page, key, value );
	
	//we may have just one child_page, may have two
	//merge the results
	
	//if too many keys 
	//TODO how many is too many
	    if (node.is_root) {
		//pick midle as new root
		//write halves as internal nodes
		//write root pointing to internals nodes
		//return [ root_page_number ]
		
	    }
	    else { 
		//node.is_interntal
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

var Node = function Node( json_obj ){
    this.is_root = json_obj.type == NODE_TYPES.ROOT;
    this.is_internal = json_obj.type == NODE_TYPES.INTERNAL;
    this.is_leaf = json_obj.type == NODE_TYPES.LEAF;

    this.keys = json_obj.keys;

    if (this.is_leaf) {
	this.values = jsob_obj.values;
    }
    else {
	this.children  = json_obj.children;
    }
   

    
};


/* Return the serialized representation of this node 
*/
Node.prototype.serialize = function serialize() {
    

};


/* Merge the key/value into this leaf node. 
   In the case of an internal node, the value is the child page number of the next node in the tree.
*/
Node.prototype.merge_in = function merge_in( key, value ) {

};

/* Split this node in twain, usually because it's become too big.
 */
Node.prototype.split = function split() {

}