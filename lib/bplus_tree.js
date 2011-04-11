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

//TODO how is data going to be stored in the block?
//     do we need to pad with zeros? 
//     so much space is wasted inside 


var NODE_TYPES = { 
    ROOT : 0,
    INTERNAL : 1,
    LEAD : 2 
};


var BPlusTree = function BPlusTree() {

}
exports.BPlusTree = BPlusTree;

BPlusTree.prototype.insert_append = function insert_append( child_page, key, value ) {

    var node;
    //slurp node from fs

    // recursion end case
    if (node.is_leaf) {
	//merge into key, value into node
	//if it is too big now
	    //split it 
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


