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
            this._tree = json[1];
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
        return  [ 1, this._tree ];
    }
};


DocHistory.prototype.append = function append( new_item, parent_item, force ) {
    
    if ( ! new_item ) { throw 'new_item is a required param' };
    
    if ( this.is_list ) { return this._append__list( new_item, parent_item, force ) }
    else if ( this.is_tree ) { return this._append__tree( new_item, parent_item, force ) }
};
 
 
DocHistory.prototype._append__list = function _append__list( new_item, parent_item, force ) {

    //if we don't have a parent item, then this is easy
    if ( ! parent_item ) {
        
        //TODO check what's being pushed?
        this._list.push( new_item );
    }
    else {
     
        //check the parent
        var last_item = this._list[this._list.length-1];
        if ( parent_item.rev === last_item.rev ) {
            
            this._list.push( new_item );
            
        }
        else if ( force ) { //uh oh, turn the list into a tree, we have a conflict
            
            //TODO implement converting to tree
            
        }
        else {
            
            throw "Conflicted detected and force wasn't set."   
        }   
    }
};


/* Return the leaves of this doc history tree sorted by height.
*/
DocHistory.prototype.leaves = function leaves( subtree, height ) {
    
    //TODO there may be a more effeceient way to do this?
    height = height || 0;    

    //recursive end case
    if ( subtree[1] === undefined ) {
        return [ [new DocHistoryItem_from_json( subtree[0] ), height ] ];
    }
     
    var children = subtree[1];
    var child_leaves = [];
    
    for ( var i; i < children.length; i++ ) {
     
        var one_child_leaves = this.leaves( children[i], height + 1 );
        child_leaves = child_leaves.concat(one_child_leaves);
    }
    

    //we really only need to sort when height is 0, but this makes it cleaner
    child_leaves.sort( function( a, b ) { return b[1] - a[1] } );
    return child_leaves;
};


DocHistory.prototype.get_current = function get_current() {
    
    if ( this.is_list ) { return this._get_current__list() }
    else if ( this.is_tree ) { return this._get_current__tree() }
};


DocHistory.prototype._get_current__list = function _get_current__list() {
    
    //easy, it's the last one
    var last_doc_history_item_json = this._list[this._list.length-1];
    return new DocHistoryItem_from_json( last_doc_history_item_json );
};


DocHistory.prototype._get_current__tree = function _get_current__tree() {

    //walk the tree depth first, pick the first, the rest are conflicting
    var child_leaves = this.leaves();
    
    var max_height = child_leaves[0][1];
    //find the winning revision
    var winning_item = new DocHistoryItem( child_leaves[0][0] );
    for ( var i = 1; i< child_leaves.length; i++ ) {
        
        if ( child_leaves[i][1] === winning_item_json[1] ) {
            var item = new DocHistoryItem( child_leaves[i][0] );
            if ( item.rev > winning_item.rev ) {
                winning_Item = item;   
            }
        }
        else { //height is less, we're done
            break;
        }
    }
    
};


DocHistory.prototype.get_rev = function get_rev( rev ) {    
    
    if ( this.is_list ) { return this._get_rev__list( rev ) }
    else if ( this.is_tree ) { return this._get_rev__tree(rev ) }
};


DocHistory.prototype._get_rev__list = function _get_rev__list( rev ) {    

    var found_item = null;
    
    for ( var i = this._list.length - 1; i > - 1; i-- ) {
        
        var doc_history_item = new DocHistoryItem( this._list[i] );
        
        if ( doc_history_item.rev === rev ) {
            found_item = doc_history_item;    
            break;
        }
    }

    return found_item;
};


DocHistory.prototype.get_rev__tree = function get_rev__tree( rev ) {    

    var found_item = null;
    var child_leaves = this.leaves();
    for ( var i; i < child_leaves.length; i++ ) {
        
        var item = new DocHistoryItem( child_leaves[i][0] );
        
        if ( item.rev === rev ) {
            found_item = item;
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
