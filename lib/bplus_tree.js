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
	//TODO special case when the root is empty

	// find the greatest  node_key such that:  key >= node_key

	// if there is such node_key and it is at position i in the key array, then the child page 
	// we want is i + 1 in the child page array

	// if there is no such node key, then the child page we want is indexed at 0 in the
	// child_page array and we let i = -1 so that i + 1 = 0. Here i is the imaginary position
	// in the array before 0

	// now we recurse
	var results = insert_append( next_child_page, key, value );
	
	/* 
	   if results contains only one value, this is the child page replacing the one at i + 1
	   otherwise we should have:
	   [ first_child_page, second_child_page, smallest_key_of_second_child_page ]

	   think of it like this:

              smallest_key_second_child_age
	        /                     \
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