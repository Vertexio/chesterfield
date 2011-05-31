var U = require('underscore');

HISTORY_TYPES = {
  LIST : 0,
  TREE : 1
};


/* DocHistory is a convenience abstraction around the JSON which 
   specifiecs a Document History Item array in the document index B+Tree.
*/
var DocHistory = function DocHistory( json ) {
  
    if ( json ) {
        
        if ( ! json instanceof Array ) {
            throw "json should be array";   
        }

        this._list = [];
        if ( json[0] === HISTORY_TYPES.LIST ) {
            this.is_list = true;
            this._list = json[1];      
        }
        else if ( json[0] === HISTORY_TYPES.TREE ) {
            this.is_tree = true;
            this._trees = json[1];
        }
    }
    else {
        
        //histories start out life as a list
        this.is_list = true;
        this._list = [];
    }
};
exports.DocHistory = DocHistory;


DocHistory.prototype.toJSON = function toJSON() {
    
    if ( this.is_list ) {
        return [ 0, this._list ];    
    }
    else if ( this.is_tree ) {
        return  [ 1, this._trees ];
    }
};


/* Return the leaves of the doc history tree sorted revision base 1st, then md5sum when they bases match.
*/
DocHistory.prototype.leaves = function leaves() {
    
    //if the tree has been compacted, there may be no single root but many subtrees
    var leaves = [];
        
    if ( this.is_list ) {
    
        var leaf_json = this._list[this._list.length-1];
        
        leaves.push( new DocHistoryItem_from_json(leaf_json ) );
    }
    else if ( this.is_tree ) {
        
        for ( var i = 0; i < this._trees.length; i++ ) {
         
            var subtree = this._trees[i];
            var subtree_leaves = this.subtree_leaves( subtree );
            leaves = leaves.concat(subtree_leaves);        
        }
        
        //TODO sorting may not agree with original couchdb sorting, they will need 
        //to if we want to intermix implementations in a replicated setup
        
        //sort the leaves first by rev_base, then by remaining md5sum
        var sort_func = function( a, b ) {
         
            //revs look like: base-md5sum
            var a_rev_components = a.rev.split( '-' );
            var b_rev_components = b.rev.split( '-' );
            
            var a_rev_base = parseInt( a_rev_components[0] );
            var b_rev_base = parseInt( b_rev_components[0] );
            
            if ( a_rev_base !== b_rev_base ) {
                //sort descending, longest history wins   
                return b_rev_base - a_rev_base;  
            }
            else {
                //sort decending based on md5sum string            
                return b_rev_components[1] > a_rev_components[1] ? 1 : b_rev_components[1] < a_rev_components ? -1 : 0;
            }
        };

    }
    //the first in the list is the 'winning' or 'current' or 'latest' revision
    leaves.sort( sort_func );  
    return leaves;
};



/* Return the leaves of the doc history subtree sorted by height.
*/
DocHistory.prototype.subtree_leaves = function subtree_leaves( subtree ) {
    
    //recursive end case
    if ( subtree[1] === undefined ) {
        return [ new DocHistoryItem_from_json( subtree[0] ) ];
    }
     
    var children = subtree[1];
    var child_leaves = [];
    
    for ( var i = 0; i < children.length; i++ ) {
     
        var one_child_leaves = this.subtree_leaves( children[i] );
        child_leaves = child_leaves.concat(one_child_leaves);
    }
    
    return child_leaves;
};


/* Returns true if the specified rev is belongs to a leaf in the document 
   history tree
*/
DocHistory.prototype.rev_is_leaf = function rev_is_leaf( rev ) {

    var leaves = this.leaves();
    var leaf_revs = U( leaves ).map(  function ( leaf ) { return leaf.rev } );
    var index_of_rev = U( leaf_revs ).indexOf( rev );
    return  index_of_rev === -1 ? false : true;
};


DocHistory.prototype.append = function append( new_item, parent_item, force ) {
    
    if ( ! new_item ) { throw 'new_item is a required param' };
    
    if ( this.is_list ) { return this._append__list( new_item, parent_item, force ) }
    else if ( this.is_tree ) { return this._append__tree( new_item, parent_item, force ) }
};
 
 
DocHistory.prototype._append__list = function _append__list( new_item, parent_item, force ) {

    //if the list is empty then this is the first revision
    if ( this._list.length === 0 ) {
        
        //TODO check what's being pushed?
        this._list.push( new_item.toJSON() );
        return true;
    }
    else {
     
        //check the parent is the last item
        var last_item = new DocHistoryItem_from_json( this._list[this._list.length-1]);
        if ( parent_item && parent_item.rev === last_item.rev ) {
            
            this._list.push( new_item.toJSON() );
            return true;
            
        }    
        else if ( force ) { 
            //if there is no parent, but we're forcing then 
            //turn the list into a tree, we're force inserting a conflict
 
            var trees = [];
            var root = [ this._list[0] ];
            trees.push( root );
            var previous_node = root;
            for ( var i = 1; i < this._list.length; i++ ) {
            
                //adding the children list and putting the first child in it
                var next_node = [ [ this._list[i] ] ];
                previous_node[1] = next_node;
                next_node = previous_node;
            }

            delete this._list;
            this.is_list = false;
            this._trees = trees;
            this.is_tree = true;
            
            //hand off to append tree now that conversion is complete
            return this._append__tree( new_item, parent_item, force );
        }
        else {
        
            //parent is required except during a force
            return false; 
        }   
    }
};


DocHistory.prototype._append__tree = function _append__tree( new_item, parent_item, force ) {

    if ( ! parent_item && ! force ) {
        
        return false;
    }
    else if ( ! parent_item && force ) {

        //if the parent and we're forcing isn't set this we create a new subtree of a single leaf
        var subtree = [ new_item.toJSON() ];     
        this._trees.push( subtree );
        return true;
    }
    else { //parent_item is set 
    
        var appended = false;
        for ( var i = 0; i < this._trees.length; i++ ) {
            
            //force is implied in subtrees
            appended = this._append__subtree( this._trees[i], new_item, parent_item );
            if ( appended ) {
               return true;  
            }
        }
        
        return appended;
    }
};


DocHistory.prototype._append__subtree = function _append__subtree( subtree, new_item, parent_item ) {

    var root_item = new DocHistoryItem_from_json( subtree[0] );
    var children = subtree[1] || [];
    
    if ( root_item.rev === parent_item.rev ) {
        
        if ( ! subtree[1] ) {
            subtree[1] =  children  
        }
        children.push( [ new_item.toJSON() ] );
        return true;
    }
    else {
        var appended = false;
        for ( var i = 0; i < children.length; i++ ) {
            
            appended = this._append__subtree( children[i], new_item, parent_item );   
            if ( appended ) {
                return true;    
            }
        }
        
        return appended;
    }
};


/* Get the current revision, aka the latest revision or winning revision.
*/
DocHistory.prototype.get_current = function get_current() {
    
    if ( this.is_list ) { return this._get_current__list() }
    else if ( this.is_tree ) { return this._get_current__tree() }
};


DocHistory.prototype._get_current__list = function _get_current__list() {
    
    //easy, it's the last one
    var last_doc_history_item_json = this._list[this._list.length-1];
    
    //deleted updates don't count as current revision
    last_doc_history_item_json.deleted === 1 ? null : last_doc_history_item_json;
    return new DocHistoryItem_from_json( last_doc_history_item_json );
};


DocHistory.prototype._get_current__tree = function _get_current__tree() {

    //leaves sorted by deterministic algo, first is the current revision
    var child_leaves = this.leaves();
    
    //this is actually a list of DocHistoryItems
    
    //TODO return the first leaf that isn't a delete, this is the current wining rev
    var non_deleted_child_leaves = U(child_leaves).select( function ( leaf ) { return leaf.deleted !== 1 } )
    return non_deleted_child_leaves[0];    
};


/* Get a DocHistoryItem by rev
*/
DocHistory.prototype.get_item_by_rev = function get_item_by_rev( rev ) {    
    
    if ( this.is_list ) { return this._get_rev__list( rev ) }
    else if ( this.is_tree ) { return this._get_rev__tree(rev ) }
};


DocHistory.prototype._get_rev__list = function _get_rev__list( rev ) {    

    var found_item = null;

    //easy, loop through all revs
    for ( var i = this._list.length - 1; i > - 1; i-- ) {
        
        var doc_history_item = new DocHistoryItem_from_json( this._list[i] );
        
        if ( doc_history_item.rev === rev ) {
            found_item = doc_history_item;    
            break;
        }
    }

    return found_item;
};


DocHistory.prototype._get_rev__tree = function _get_rev__tree( rev ) {    

    var found_item = null;
    for ( var i = 0; i < this._trees.length; i++ ) {
        
        found_item = this._get_rev__subtree( this._trees[i], rev );
        if ( found_item ) { 
            break 
        };
    }
    
    return found_item;
};


DocHistory.prototype._get_rev__subtree = function _get_rev__subtree( subtree, rev ) {    
    
    //recursive endcase    
    var root_item = new DocHistoryItem_from_json( subtree[0] );
    if ( root_item.rev === rev ) {
        return root_item;   
    }
    
    var found_item = null;
    var children = subtree[1] || [];
    for ( var i = 0; i < children.length; i++ ) {
        
        var child_subtree = children[i];
        found_item = this._get_rev__subtree( child_subtree, rev );
        if ( found_item ) {
            
            break;   
        }
     }
  
    return found_item;
};


/* DocHistoryItem is a convenience abstraction around the JSON which 
   specifiecs a Document History Item inside the document index B+Tree.
*/
var DocHistoryItem = function DocHistoryItem( seq, rev, reference, len, deleted ) {
    this.seq = seq;
    this.rev = rev;
    this.reference = reference;
    this.len = len;
    this.deleted = deleted;    
};
exports.DocHistoryItem = DocHistoryItem;


/* DocHistory constructor that takes the JSON serialized to the DB.    
*/
var DocHistoryItem_from_json = function DocHistoryItem_from_json( json ) {
    return new DocHistoryItem( json[0], json[1], json[2], json[3], json[4] );
};
exports.DocHistoryItem_from_json = DocHistoryItem_from_json;


/* JSON Serialization of a Document's History
*/
DocHistoryItem.prototype.toJSON =  function toJSON() {
                
    if (this.deleted) { 
        return [this.seq, this.rev, this.reference, this.len, this.deleted];
    }
    else {
        return [this.seq, this.rev, this.reference, this.len ];
    }
};
