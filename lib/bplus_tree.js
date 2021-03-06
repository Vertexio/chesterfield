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
var generate_uuid = require('node-uuid');


var NODE_TYPES = { 
    ROOT : 0,
    INTERNAL : 1,
    LEAF : 2 
};


// Comparison function
var DEFAULT_CMP = require('./comparison.js').cmp;


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
  

    //this is our page being written 'lock', while a page is being written you must queue further writes
    this.page_write_in_progress = false;


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


/* Close the B+Tree and any file descriptors associated with it
*/
PageManager.prototype.close = function close( on_closed ) {            
    fs.close( this.fd, on_closed );
};


/* Returns true if the tree is empty, false otherwise.
*/
PageManager.prototype.get_last_page_number = function get_last_page_number() {
    return this.next_page_number - 1;
};


/* Get a node at a particular page number.
   callback: on_node_read( err, node )
*/
PageManager.prototype.get_node_at = function get_node_at( page_number, on_node_read ) {

    var file_offset = page_number * this.page_size;

    var that = this;

    var on_read = function on_read( err, bytesRead, buffer ) {

        if ( err ) {
            return on_node_read( err );
        }
        else if ( bytesRead != that.page_size ) {
            return on_node_read( 'Could not read the entire page.' );
        }
        else {
            var node;
            try { 
                node = Node.from_buffer( buffer );
            }
            catch ( err ) {
                return on_node_read( err );
            }
            return on_node_read( null, node );
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
   callback: on_page_written( err, page_number );
*/
PageManager.prototype.write_page = function write_page( page, on_page_written ) {
    
    var that = this;

    //if a page write is already in progress, queue the next write
    if ( this.page_write_in_progress ) {
	process.nextTick( function() { that.write_page( page, on_page_written ); } );
	return
    }

    this.page_write_in_progress = true;
    
    var off_set = this.next_page_number * this.page_size;

    var on_written = function on_written( err, written, buffer ) {

	//next tick no matter what, we release the 'lock'
	process.nextTick( function() { that.page_write_in_progress = false } );

	if ( err ) {
	    return on_page_written( err );
	}
        
        if ( written != buffer.length) {
            return on_page_written( 'Failed to write the entire buffer' );
        }
        else {
            var page_number = that.next_page_number;
	    
            that.next_page_number++;

            return on_page_written( null, page_number );
        }

    };

    var buffer = new Buffer(this.page_size);
    //padd with 0's
    this.EMPTY_PAGE.copy(buffer, 0, 0 ); 
    buffer.write(page, 0, 'binary');
    fs.write(this.fd, buffer, 0, buffer.length, off_set, on_written )

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
var BPlusTree = function BPlusTree( file, page_size, cmp ) {
    //
    if ( ! file ) {
	throw "file is a required param"
    }
    
    //sequence number starts at -1, first is 0
    this.sequence_number = -1;
    
    this.file = file;
    this.page_size = page_size || 4096;
    this.page_manager = new PageManager( this.file, this.page_size );

    //flag for determining if an async insert is progress, kind of like a lock, though new items
    //get queued on the event loop
    this.modification_in_progress = false;

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

    this.cmp = cmp ? cmp : DEFAULT_CMP;

    this.modification_queue = [];
        

};
exports.BPlusTree = BPlusTree;


/* Close the B+Tree and any file descriptors associated with it
*/
BPlusTree.prototype.close = function close( on_closed ) {            
    this.page_manager.close( on_closed );
};



/* Close the B+Tree and any file descriptors associated with it
*/
BPlusTree.prototype.is_empty = function is_empty() {            
    
    return this.root_page_number == -1 ? true : false;
};


/* Retrieve a value from the B+Tree given it's key
   callback: on_value_retrieved( err, value )
*/ 
BPlusTree.prototype.retrieve = function retrieve( key, on_value_retrieved ) {

    var that = this;

    if ( ! this.root_page_number || this.root_page_number == -1 ) {
	return on_value_retrieved( "Tree is empty.");
    }

    this.retrieve_from( this.root_page_number,  key, on_value_retrieved );

};


/* Retrieve a value from the given B+Tree page
   callback: on_value_retrieved( err, value )
*/ 
BPlusTree.prototype.retrieve_from = function retrieve_from( page, key, on_value_retrieved ) {
    
    var that = this;

    var on_node_read = function on_node_read( err, node )  {

	if ( key == 'foo321' ) {
	    page && key;
	}

	if ( err ) {
	    return on_value_retrieved( err );
	}

	if ( node.is_leaf ) { //end case

	    //Leaf nodes always store the payload in the same index as the key.
	    var index_of_key = U(node.keys).indexOf( key, true, that.cmp );
	    
	    if( index_of_key == -1 ) {
		return on_value_retrieved( 'Key not found: ' + key );
	    }
	    else {
		var value = node.payloads[index_of_key];
		return on_value_retrieved( null, value );
	    }

	}
	else { //recurse
	    
	    //find the correct child and recurse
	    var index_of_child_to_traverse = that.index_of_child_to_traverse( node, key );
	    var child_page = node.payloads[index_of_child_to_traverse];
	    
	    if ( child_page == null ) {
		return on_value_retrieved( 'Key not found' ); 
	    }
	    else {
		that.retrieve_from( child_page, key, on_value_retrieved );
	    }

	}
	

    };

    this.page_manager.get_node_at( page, on_node_read );


};

/* Walk the tree passing each key value pair between from_a and to_z to the emit callback and calling 
   on_walk_complete if there's an error or we're finished.
   callbacks:
      emit( key, value )
      on_complete( err )
*/
BPlusTree.prototype.walk = function walk( emit, on_walk_complete, from_a, to_z ) {

    var that = this;
    
    if ( ! this.root_page_number || this.root_page_number == -1 ) {
	return on_walk_complete( "Tree is empty.");
    }    

    this.walk_subtree( this.root_page_number, emit, on_walk_complete, from_a, to_z );

};


/* Walk the subtree passing each key value pair between a and z to the emit callback and calling 
   on_complete if there's an error or we're finished.
   callbacks:
      emit( key, value )
      on_walk_complete( err )
*/
BPlusTree.prototype.walk_subtree = function walk_subtree( page_number, emit,  on_walk_complete, 
							  from_a, to_z ) {

    var that = this;

    //dead end
    if (page_number == null || page_number == undefined ) {
	return on_walk_complete( null );
    }

    //because all leaf nodes are at the same height, we only need to create the one call back
    //which walks left to write, eventually leaf nodes are hit in order and we emit 
    var on_node_read = function on_read( err, node ) {

	if ( node.is_leaf ) {

	    //want the first key that is >= from_a
	    var start_index = from_a == undefined ? 0 
                                                  : U(node.keys).indexOf( from_a, true, that.cmp );
	    if ( start_index == -1 ) {
		start_index = U(node.keys).sortedIndex( from_a, that.cmp ); 
	    }
	                                          
	    //want the last key that is <= to_z 
	    var end_index = to_z == undefined ? node.keys.length - 1
	                                      : U(node.keys).indexOf( to_z, true, that.cmp );
	    if ( end_index == -1 ) {
		end_index = U(node.keys).sortedIndex( from_a, that.cmp ) - 1; 
	    }

	    for ( var i = start_index; i < end_index + 1; i++ ) {
		emit( node.keys[i], node.payloads[i] );
	    }
	    
	    return on_walk_complete( null );

	}
	else { //root or internal

	    var child_start_index;
	    if ( from_a == undefined ) {
		child_start_index = 0;
	    }
	    else {
		var index_of = U(node.keys).indexOf( from_a, true, that.cmp );
		child_start_index = index_of != -1 ? index_of
		                                 : U(node.keys).sortedIndex( from_a, that.cmp );
	    }
	    
	    var child_end_index;
	    if ( to_z == undefined ) {
		child_end_index = node.keys.length; //last key + 1
	    }
	    else {
		var index_of = U(node.keys).indexOf( to_z, true, that.cmp );
		child_end_index = index_of != -1 ? index_of
		                                 : U(node.keys).sortedIndex( from_a, that.cmp );
	    }

	    var total_children = total_children = child_end_index - child_start_index + 1;
	    var walked_subtrees = 0;
	    var on_recursive_walk_complete = function on_recursive_walk_complete( err ) {
		
		walked_subtrees++;

		if ( err ) {
		    return on_walk_complete( err );
		}
		
		//we can only return our on_walk_complete once all subtree's have completed
		if ( walked_subtrees == total_children ) {
		    return on_walk_complete( null );
		}
	    };

	    for ( var i = child_start_index; i < child_end_index + 1; i++ ) {
		that.walk_subtree( node.payloads[i], emit, on_recursive_walk_complete, from_a, to_z);
	    }

 	}//else
    };

    this.page_manager.get_node_at( page_number, on_node_read );

};



//TODO move this to the node object node as it functionality is similar to merge and split
/* Given an internal, or root node, this will return the index to traverse in search of the given key.
*/
BPlusTree.prototype.index_of_child_to_traverse = function _select_child_to_traverse( node, key ) {

            /* The tree looks like: 
               
               | keys[1] | keys[2] | .... | key[n] |
               |    \    |    \       \   |                             \
               |   p[1]  |   p[2]  | .... |  p[n]  | p[n+1] |   

               That is:
                  | key[i] |
                    /    \
               | p[i] | p[i+1]

               Find the greatest i such that: key >= keys[i]
               
               if there is such i, we follow payloads[i+1].
               
               if there is no such node key, then we follow p[0].
            */

    var index_of = U(node.keys).indexOf( key, true, this.cmp );

    if (index_of != -1 ) {
	return index_of + 1;
    }
    else {
	return U(node.keys).sortedIndex( key, this.cmp );
    }
 };

 /* Insert a new key/value into the B+Tree
    callback: on_node_written( err, page_number )
 */ 
 BPlusTree.prototype.write_node = function write_node( node, on_node_written ) {

     var page = node.serialize();

     var on_page_written = function on_page_written(err,  page_number ) {


	 if ( err ) {
	     return on_node_written( err );
	 }
	 else {
	     return on_node_written( undefined, page_number );
	 }
     };

     this.page_manager.write_page( page, on_page_written );
 };


 /* Insert a new key/value into the B+Tree
    callback: on_inserted( err, sequence_number )
 */ 
 BPlusTree.prototype.insert = function insert( key, value, on_inserted ) {

     var that = this;
     //if tree modification is in progress, there will soon be a new root page, so we queue up this 
     //insertion
     if ( this.modification_in_progress ) {
	 var queued_insertion = function queued_insertion() {
	     try {
		 that.insert( key, value, on_inserted ); 
	     } catch ( err ) {
		 on_inserted( err );
	     }
	 };

	 this.modification_queue.push(queued_insertion);
	 return;
     }

     //after we insert anything, we want to process the next in the insertion queue
     //this the callback we will use to do so
     var process_next_in_queue = function process_next_in_queue() { 
	 that.modification_in_progress = false; 
	 var queued_modification = that.modification_queue.shift();
	 if ( queued_modification ) { queued_modification(); }
     };


     //set the 'lock'
     this.modification_in_progress = true;

     //special case when root node is empty, write the leaf node, then write the root
     if ( this.root_page_number == -1 ) {

	 var on_inserted_wrapped = function on_inserted_wrapped( err, sequence_number) {

	     //next tick we want to reset the insert in progress then call the next insert
	     process.nextTick( process_next_in_queue );
	     
	     return on_inserted( err, sequence_number  );
	 };

	 this._insert_first_item(key, value, on_inserted_wrapped );
     }
     else {

	 var on_insert_ated = function on_insert_ated( err, page_number, node ) {

	     if ( err ) {

		 //next tick we want to reset the insert in progress then call the next insert
		 process.nextTick( process_next_in_queue );
		 return on_inserted( err );
	     }
	     else if ( page_number )  {

		 //next tick we want to reset the insert in progress then call the next insert
		 process.nextTick( process_next_in_queue );

		 that.root_page_number = page_number;

		 //TODO figure out sequence number jazz
		 var sequence_number = 0;

		 return on_inserted( null, sequence_number );
	     }
	     else { //node is defined, the root was split, write it

		 var on_root_written = function on_root_written( err, page_number ) {

		     //next tick we want to reset the insert in progress then call the next insert
		     process.nextTick( process_next_in_queue );

		     if ( err ) {
			 return on_inserted( err );
		     }

		     that.root_page_number = page_number;

		     //TODO figure out sequence number jazz
		     var sequence_number = 0;

		     return on_inserted( null, sequence_number );

		 };

		 that.write_node( node, on_root_written );

	     }


	 };
	 this.insert_at( this.root_page_number, key, value, on_insert_ated )
     }

 };


 /* Helper function for inserting the first node into a tree.
    callback: on_inserted( err, sequence_number );
 */
 BPlusTree.prototype._insert_first_item = function _insert_first_item( key, value, on_inserted ) {

     var that = this;

     //once we've written a leaf node with the key+value, we'll want to write a root node as well                                                                                                                                          
     var on_leaf_written = function on_leaf_written(err, page_number ) {

	 if ( err ) {
	     return on_inserted( err )
	 }

	 var root = new Node( NODE_TYPES.ROOT, [key], [null, page_number]  );

	 //once we've written the root note, call on_inserted callback
	 var on_root_written = function on_root_written( err, page_number ) {

	     if ( err ) {
		 return on_inserted( err )
	     }
	     else {
		 that.root_page_number = that.page_manager.get_last_page_number();
		 
         that.sequence_number = 0;
         
		 return on_inserted( null, 0 );
	     }

	 };

	 that.write_node( root, on_root_written );
     };


    //value maybe a function providing a value, this what we're going to do either way
    var continuation = function continuation ( err, value_from_function ) {
     
        if ( err ) {
            return on_leaf_written( err );
        }
     
        var leaf = new Node( NODE_TYPES.LEAF, [key], [value_from_function] );
        that.write_node( leaf, on_leaf_written );
    };
    
    //value can be a function, in which case it's a 'value bridge' of form
    //bridge( context, continuation ), the continuation is called asynchronously
    if ( value instanceof Function ) {
        
        var context = { 'key' : key };
        value( context, continuation );
    }
    else {
        continuation( null, value );   
    }
     
 };


//TODO this algorithm doesn't attempt to balance the leaf nodes.
//That is set a minium for the number of items in a leaf node and rejigger the tree in order to enforce it.
//Is it worth doing so?
//the advantage is a more compact/shallower tree => less disk hits to lookup items
//the disadvantage is complexity, potentially more writes during inserts due to rejiggering, more space
//under SSD the advantages may not be as pronounced and space is more expensive
//further more the code is already quite complex


 /* Inserts a new key/value pair into sub tree refenced by page_number
    callback: on_inserted( err, page_number, new_parent ) 

 */
 BPlusTree.prototype.insert_at = function insert_at( page_number, key, value, on_inserted ) {

     var that = this;

     //reading nodes from the B+Tree is completely asynchronous, here's what we're going to do
     //once the node has been read
     var on_node_read = function on_node_read( err, node ) {

	 if ( err ) {
	     return on_inserted( err );
	 }

	 // recursion end case
	 if (node.is_leaf) {

	     //see if the key exists using a binary search
	     var index_of_key = U(node.keys).indexOf( key, true, that.cmp );

	     if ( index_of_key != -1  ) { //key exists, bail
		 var err =  'Key already exists!';
		 return on_inserted( err );
	     }

	     // create a node from key, value and merge it in
	     // TODO we could use use this if page_number was null but this is clean and easy for now
	     
         //value could be a function, so we need to make a continuation now
         //this get's called no matter what
         var continuation = function continuation( err, value ) {
           
            if ( err ) {
                
                return on_inserted( err );   
            }
             
            var node_to_merge = new Node( NODE_TYPES.LEAF, [key], [value] );
            node.merge( node_to_merge );

	        //TODO special case, if the node merged node has two keys and is now too big, 
	        //there's no point splitting it later and writing both halves


	        //use helper to write node, passing either a page_number or a parent node up call stack 
	        //to be merged
    	    var on_nodes_written = function on_nodes_written( err, page_number, new_parent ) {

		        return on_inserted( err, page_number, new_parent );
	        };//on_nodes_written
	        that._write_nodes_helper( node, on_nodes_written )            

         }; //continuation
        
         if ( value instanceof Function ) {
            var context = { 'key' : key };
            value( context, continuation );
         }
         else {
            continuation( null, value );
         }
         
	  }
	  else { 

	      var index_of_child_to_traverse = that.index_of_child_to_traverse( node, key );

	      var child_page_number = node.payloads[index_of_child_to_traverse];

	      /* Almost ready to recurse, but first we setup the call back as it does all the work.
		 This is a bit difficult to visualize, but essentially you're creating a callback
		 chain as you recurse looking for the right leaf node. Eventually you hit the bottom of 
		 the tree, then the callbacks fire all the way back up writing nodes and creating a new 
		 tree.
	      */
	      var on_recursive_insert = function on_recursive_insert(err, page_number, new_parent ) {
		  if ( err ) {
		      //TODO some kind of error handing/logging here?
		      return on_inserted( err );
		  }
		  else if ( new_parent ) { //a new parent node is bubbling up from below
		      node.merge( new_parent );
		  }
		  else if ( page_number || page_number == 0 ){ //the child page replacing the one we traversed
		      node.payloads[index_of_child_to_traverse] = page_number;
		  } 
		  else {
		      return on_inserted( 'Unexpected error has occured' );
		  }

		  if (node.is_root) {
		      //TODO add the new sequence number to the parent's metadata?
		  }

		  //once we write this node, we will either end up with a page_number
		  //or a new_parent to bubble up
		  var on_nodes_written =  function on_nodes_written( err, page_number, new_parent ) {

		      return on_inserted( err, page_number, new_parent )

		  }; //on_nodes_written

		  that._write_nodes_helper( node, on_nodes_written )

	      };

	      that.insert_at( child_page_number, key, value, on_recursive_insert );

	  }
      };

      //now that we know what we're going to do with that node, we need to get it
      var node;
      if ( ! page_number && page_number != 0 ) { //create a new empty leaf node , this is the recursive end case
	  node = new Node( NODE_TYPES.LEAF );
	  return on_node_read( null, node );
      }
      else { //slurp node from fs
	  return this.page_manager.get_node_at( page_number, on_node_read );
      }

  };


 /* Update the value associated with a key the B+Tree.
    callbacks:
        value( old_value ) //optional, value can also be an object or primative
        on_updated( err, sequence_number )
 */ 
 
//TODO better update tests
BPlusTree.prototype.update = function update( key, value, on_updated ) {

    var that = this;

    //if tree modification is in progress, there will soon be a new root page, so we queue up this 
    //update
    if (this.modification_in_progress) {
        var queued_update = function queued_update() {
                try {
                    that.update( key, value, on_updated );
                }
                catch (err) {
                    on_updated(err);
                }
            };

        this.modification_queue.push( queued_update ) ;
        return;
    }

    //after we update anything, we want to process the next in the modification queue
    //this the callback we will use to do so
    var process_next_in_queue = function process_next_in_queue() {
            that.modification_in_progress = false;
            var queued_modification = that.modification_queue.shift();
            if (queued_modification) {
                queued_modification();
            }
        };


    //set the 'lock'
    this.modification_in_progress = true;

    //root node is empty, nothing to update
    if (this.root_page_number == -1) {
        return on_updated( 'Root is empty, key not found' );
    }
    else {

        var on_update_ated = function on_update_ated( err, page_number, node ) {

                if (err) {

                    //next tick we want to reset the insert in progress then call the next insert
                    process.nextTick(process_next_in_queue);
                    return on_updated(err);
                }
                else if ( page_number ) {

                    //next tick we want to process the next in the modification queue
                    process.nextTick(process_next_in_queue);

                    that.root_page_number = page_number;

                    //TODO figure out sequence number jazz
                    var sequence_number = 0;

                    return on_updated(null, sequence_number);
                }
                else { //node is defined, the root was split, write it
                    var on_root_written = function on_root_written(err, page_number) {

                           //next tick we want to process the next in the modification queue
                            process.nextTick(process_next_in_queue);

                            if (err) {
                                return on_updated(err);
                            }

                            that.root_page_number = page_number;

                            //TODO figure out sequence number jazz
                            var sequence_number = 0;

                            return on_updated(null, sequence_number);

                    };
                    that.write_node(node, on_root_written);

                }


        };
        this.update_at(this.root_page_number, key, value, on_update_ated)
    }

};


//TODO this algorithm doesn't attempt to balance the leaf nodes.
//That is set a minium for the number of items in a leaf node and rejigger the tree in order to enforce it.
//Is it worth doing so?
//the advantage is a more compact/shallower tree => less disk hits to lookup items
//the disadvantage is complexity, potentially more writes during inserts due to rejiggering, more space
//under SSD the advantages may not be as pronounced and space is more expensive
//further more the code is already quite complex


 /* Updates the value associated with a the key provided in the sub tree refenced by page_number
    callbacks: 
        on_updated( err, page_number, new_parent ) 
        value( old_value )

 */
 BPlusTree.prototype.update_at = function update_at(page_number, key, value, on_updated) {

    var that = this;

    //reading nodes from the B+Tree is completely asynchronous, here's what we're going to do
    //once the node has been read
    var on_node_read = function on_node_read(err, node) {

            if (err) {
                return on_updated(err);
            }

            // recursion end case
            if (node.is_leaf) {

                //see if the key exists using a binary search
                var index_of_key = U(node.keys).indexOf(key, true, that.cmp);

                if (index_of_key == -1) { //key does not exist
                    var err = 'Key does not exist!';
                    return on_updated(err);
                }

                var old_value = node.payloads[index_of_key];
  
                //value could be a function, here's what's going to happen regardless
                var continuation = function continuation( err, value ) {

                    if ( err ) {
                        return on_updated( err );
                    }

                    //overwrite the new value with a transformed valued                
                    node.payloads[index_of_key] = value;

                    //use helper to write node, passing either a page_number or a parent node up call stack 
                    //to be merged
                    var on_nodes_written = function on_nodes_written(err, page_number, new_parent) {

                        return on_updated(err, page_number, new_parent);

                    }; //on_nodes_written
                    that._write_nodes_helper(node, on_nodes_written);
                };// continuation
                
                if ( value instanceof Function ) {                 
                    var context = { 'key' : key, 'old_value' : old_value };
                    value( context, continuation);                    
                }
                else {
                    continuation( null, value );   
                }

            }
            else {

                var index_of_child_to_traverse = that.index_of_child_to_traverse(node, key);

                var child_page_number = node.payloads[index_of_child_to_traverse];

                /* Almost ready to recurse, but first we setup the call back as it does all the work.
		           This is a bit difficult to visualize, but essentially you're creating a callback
		           chain as you recurse looking for the right leaf node. Eventually you hit the bottom of 
		           the tree, then the callbacks fire all the way back up writing nodes and creating a new 
		            tree.
	            */
                var on_recursive_update = function on_recursive_update(err, page_number, new_parent) {
                        if (err) {
                            //TODO some kind of error handing/logging here?
                            return on_updated(err);
                        }
                        else if (new_parent) { //a new parent node is bubbling up from below
                            node.merge(new_parent);
                        }
                        else if (page_number || page_number == 0) { //the child page replacing the one we traversed
                            node.payloads[index_of_child_to_traverse] = page_number;
                        }
                        else {
                            return on_updated('Unexpected error has occured');
                        }

                        if (node.is_root) {
                            //TODO add the new sequence number to the parent's metadata?
                        }

                        //once we write this node, we will either end up with a page_number
                        //or a new_parent to bubble up
                        var on_nodes_written = function on_nodes_written(err, page_number, new_parent) {

                                return on_updated(err, page_number, new_parent)

                            }; //on_nodes_written
                        that._write_nodes_helper(node, on_nodes_written)

                };
                that.update_at(child_page_number, key, value, on_recursive_update);

            }
        };

    //now that we know what we're going to do with that node, we need to get it
    var node;
    if (!page_number && page_number != 0) { //create a new empty leaf node , this is the recursive end case
        node = new Node(NODE_TYPES.LEAF);
        return on_node_read(null, node);
    }
    else { //slurp node from fs
        return this.page_manager.get_node_at(page_number, on_node_read);
    }

};

  /* Write's an internal node to disk. If that node is too big, it splits it up, writes the children
     and returns the parent.
     callback: on_written( err, page_number, new_parent ) 
	 page_number will be the page number that was updated otherwise
	 new_parent will be the new parent if we need to merge

  */
  BPlusTree.prototype._write_nodes_helper = function _write_nodes_helper( node, on_written ) {

      //Check the size of node
      if ( node.serialize().length > this.page_size ) {
	  var components = node.split();
	  var parent_key = components[0];
	  var left_child = components[1];
	  var right_child = components[2];

	  var new_parent_type = node.is_leaf || node.is_internal ? NODE_TYPES.INTERNAL : NODE_TYPES.ROOT;

	  var new_parent = new Node( new_parent_type, [parent_key], [] );
	  var errors = [];

	  //callback for writing node all three nodes
	  var on_node_written = function on_node_written(err, page_number ) {

	      if ( err ) {
		  return on_written(err);
	      }

	      //we add the child_page_numbers to new_parent, afer the second write we fire the callback given above
	      new_parent.payloads.push( page_number );

	      if ( new_parent.payloads.length == 2 ) {
		  //call callback given to .write_nodes_helper since we've added both children now
		  //parent is to be merged above
		  return on_written(null, null, new_parent );
	      }

	  }; //end on_node_written

	  //write the files and pass the callback we just created
	  this.write_node( left_child, on_node_written );
	  this.write_node( right_child, on_node_written );
      }
      else { // there was no need to split

	  //call back for write_node
	  var on_node_written = function on_node_written( err, page_number  ) {

	      //call callback given to .write_nodes_helper way above, we have a page_number if all
	      //went well
	      return on_written( err, page_number );

	  }; // end on_node_written

	  //write the files
	  this.write_node(node, on_node_written);
      }

  };

/* Delete an item from the tree currently implemented as lazy deletion.
*/

BPlusTree.prototype.delete = function _delete( key, on_deleted, test ) {

     var that = this;
     //if tree modification is in progress, there will soon be a new root page, so we queue up this 
     //insertion
     if ( this.modification_in_progress ) {
	 var queued_deletion = function queued_deletion() {
	     try {
		 that.delete( key, on_deleted, test ); 
	     } catch ( err ) {
		 on_deleted( err );
	     }
	 };

	 this.modification_queue.push(queued_deletion);
	 return;
     }

     //after we delete anything, we want to process the next in the modification queue
     //this the callback we will use to do so
     var process_next_in_queue = function process_next_in_queue() { 
	 that.modification_in_progress = false; 
	 var queued_modification = that.modification_queue.shift();
	 if ( queued_modification ) { queued_modification(); }
     };


     //set the 'lock'
     this.modification_in_progress = true;


     if ( this.root_page_number == -1 ) {
	 throw "Tree is empty";
     }

     var on_deleted_from_root = function on_deleted_from_root( err, page_number, new_leftmost_key ) {

	 //next tick we want to reset the insert in progress then call the next insert
	 process.nextTick( process_next_in_queue );

	 if ( err ) {
	     
	     return on_deleted( err );
	 }
	 
     
	 if ( page_number ) {

	     that.root_page_number = page_number;

	     return on_deleted( null );

	 }
	 else { //tree now empty?

	     //TODO we should still write an empty root as a placeholder otherwise on restart there will still be
	     //the last node in the tree
	     that.root_page_number = -1;
	     return on_deleted( null );
	 }

     };

     this.delete_from( this.root_page_number, key, on_deleted_from_root, test );
};


/* Delete from the subtree 'rooted' at page_number using a lazy deletion strategy.
   callback: on_deleted_from( err, page_number, new_leftmost_key )
*/
BPlusTree.prototype.delete_from = function delete_from( page_number, key, on_deleted_from, test ) {

    var that = this;

    var on_node_read = function on_node_read( err, node ) {

	if ( err ) {
	    return on_deleted_from( err );
	}

	//recursion end case
	if ( node.is_leaf ) {
	    
	    //see if the key exists using a binary search
	     var index_of_key = U(node.keys).indexOf( key, true, that.cmp );

	     if ( index_of_key == -1  ) { //key does not exist, bail
		    var err =  'Key does not exist.';
		    return on_deleted_from( err );
	     }

         //a test might be defined, this will happen regardless
         var continuation = function continuation( err ) {
             
            if ( err ) {
                return on_deleted_from( err );   
            } 
        
	        //remove the key and payload from the node
	        node.keys.splice( index_of_key, 1 );
	        node.payloads.splice( index_of_key, 1 );

	        //if the leaf is now empty we're done, there's no new page to write, nor is there a new leftmost key
	        if ( node.keys.length == 0 ) {
		        return on_deleted_from( null, null, null );
	        }

	        //if we just removed the left most key, the parent key pointing as this leaf needs the new left most key
	        var new_leftmost_key = null;
	        if ( index_of_key == 0 ) {
		    new_leftmost_key = node.keys[0];
	        }

	        //after we've written this node to disk... 
	        var on_node_written = function on_node_written( err, page_number )  {		 
    		    return on_deleted_from( null, page_number, new_leftmost_key );
	        };	     
	        that.write_node( node, on_node_written );
            
         }; //continuation
         
         //if test was defined, run it and pass along the continuation
         if ( test instanceof Function ) {
            var context = { 'key' : key };
            test( context, continuation );
         }
	     else {
             continuation( null );   
	     }
	}
	else { //internal or root node

	    var index_of_child_to_traverse = that.index_of_child_to_traverse( node, key );

	    var child_page_number = node.payloads[index_of_child_to_traverse];

	    var on_recursive_delete_from = function on_recursive_delete_from( err, page_number,  new_leftmost_key ) {

            if ( err ) {
                return on_deleted_from( err );   
            }

		//this is the new_leftmost_key we pass up, as opposed to the one we receive
		var new_new_leftmost_key = null;

		if ( page_number != null ) { 

		    node.payloads[index_of_child_to_traverse] = page_number;
		}

		if ( index_of_child_to_traverse == 0 ) { //special case, the left most child was traversed
		    
		    if ( page_number == null ) { //left most child now entirely gone? 
			
			if ( node.keys.length == 0 ) { //is this whole node gone, or just the left most child?

			    node = null;
			}
			else { 

			    node.payloads[0] = null;
			    new_new_leftmost_key = node.keys[0]; //this get's passed up, as its now the leftmost key

			}

		    }
		    else if ( new_leftmost_key != null ) { //still need to pass the new left most key on up

			new_new_leftmost_key = new_leftmost_key;
			
		    }
		}
		else {  //not payload[0]
		    
		    if ( page_number == null ) { //child now gone, remove payload and key
			
			node.payloads.splice( index_of_child_to_traverse, 1 );
			node.keys.splice( index_of_child_to_traverse - 1, 1 );
			
			//check that the node isn't empty now
			if ( node.keys.length == 0 && node.payloads[0] == null ) {

			    node = null;
			
			}
		       
		    }
		    else if ( new_leftmost_key != null ) { //child now only contains keys >= to the new_leftmost_key
			
			node.keys[index_of_child_to_traverse - 1 ] = new_leftmost_key;

		    }
		    
		}

		//now, our node looks correct and new_newleft_most is set if need be
		if ( node == null ) { //write it if necessary

		    return on_deleted_from( null, null, new_new_leftmost_key );

		} 
		else { //write it
		    
		    var on_node_written = function on_node_written( err, page_number ) {

			return on_deleted_from( null, page_number, new_new_leftmost_key );

		    };
		    
		    return that.write_node( node, on_node_written );
		}

	    };
	    
	    return that.delete_from( child_page_number, key, on_recursive_delete_from, test );

	} //else

    };

    this.page_manager.get_node_at( page_number, on_node_read );

};



/* Node constructor.
*/
var Node = function Node( type, keys, payloads, metadata, cmp ){

      this.type = type;    
      this.is_root = type === NODE_TYPES.ROOT;
      this.is_internal = type === NODE_TYPES.INTERNAL;
      this.is_leaf = type === NODE_TYPES.LEAF;

      this.keys = keys || [];
      this.payloads = payloads || [];

      if ( metadata ) {
	  this.metadata = metadata;
      }

      this.cmp = cmp ? cmp : DEFAULT_CMP;

  };


  /* Constructor that takes a serialized json obj representing a node.
  */
  Node.from_string = function node_from_string( string ) {

      var json_obj = JSON.parse( string );
      return new Node( json_obj.type, json_obj.keys, json_obj.payloads, json_obj.metadata );
  };


  /* Constructor that takes a buffer containing serialized json_obj representing a node.
  */
  Node.from_buffer = function node_from_buffer( buffer ) {

      var buffer_string = buffer.toString();
      var last_index = buffer_string.lastIndexOf('}');
      var object_string = buffer_string.slice( 0, last_index+1 );

      var node = Node.from_string( object_string );
      return node;
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

      var json_string = JSON.stringify(json_obj);
      return json_string;
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

      if ( this.type == node_to_merge.type && this.type == NODE_TYPES.LEAF ) {
	  //find where to insert the key/payload
	  var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[0], this.cmp );
	  this.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	  this.payloads.splice( insertion_index, 0, node_to_merge.payloads[0] );
      }
      else if ( ( this.is_root || this.is_internal ) && node_to_merge.is_internal ) {
	  //the insertion_index tells us where the key goes, it also which child
	 //gets replaced as it was the one traversed
	 //another way of think of it is this child turns into two
	 var insertion_index = U(this.keys).sortedIndex( node_to_merge.keys[0], this.cmp );
	 this.keys.splice( insertion_index, 0, node_to_merge.keys[0] );
	 this.payloads[insertion_index] = node_to_merge.payloads[0];
	 this.payloads.splice( insertion_index + 1, 0, node_to_merge.payloads[1] );
     }
     else {
	 throw "Merging types not allowed";
     }


 };


/* Split this node in twain, usually because it's become too big.
 */
Node.prototype.split = function split() {

    //TODO maintain metadata?
    if( ! this.is_leaf && this.keys.length <= 2 ) {
	throw "Splitting interal/root nodes with 2 or less keys not currently supported";
    }

    //first find the middle key
    var half_length = Math.floor(this.keys.length/2);

    //get elements up to and including the middle_index, + 1 accounts for the array starting at 0
    var keys_left = U(this.keys).first( half_length );
    var payloads_left = U(this.payloads).first( half_length );

    //get the rest after the first half_length indices
    var keys_right = U(this.keys).rest( half_length );
    var payloads_right = U(this.payloads).rest( half_length );

    if ( this.is_leaf ) {
	var child_type = NODE_TYPES.LEAF;
	var parent_key = keys_right[0];
    }    
    else  {
	//root nodes split into internal nodes
	var child_type = NODE_TYPES.INTERNAL; 

	//we take the parent from the right_keys as this array will be bigger when the original node had odd length keys
	var parent_key = keys_right.shift();

	//now we must move the first payload (child) from the right to the left as everything less than the key
	//should now be referenced by the left child
	payloads_left.push( payloads_right.shift() );

    }

    //create child nodes
    var child_left = new Node( child_type, keys_left, payloads_left );
    var child_right = new Node( child_type, keys_right, payloads_right );	
    return [ parent_key, child_left, child_right ]
       
};

/* Clone a node. This provides a deep copy, but remember that nodes point to pages not other nodes.
*/
Node.prototype.clone = function clone() {

    //TODO this probably not very performant
    return Node.from_string( this.serialize() );

}

