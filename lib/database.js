var BPlusTree = require('../lib/bplus_tree').BPlusTree;
var DocumentLog = require('../lib/document_log').DocumentLog;
var generate_uuid = require('node-uuid');
var fs = require('fs');
/* Async DB Object constructor
    callback: on_ready( err ) once the db can be used

*/
var DB = function DB( db_path, create_if_missing, on_ready ) {
        
        var that = this;
        
        if ( ! db_path ) {
            throw "db_path is a required param";   
        }
        
        //TODO check the path exists        
        //TODO shoud we create if missing?        
        var path_split = db_path.split( '/' );
        
        this.name = path_split.pop;
        this.doc_index_path = db_path + '/document.index'; 
        this.seq_index_path = db_path + '/sequence_number.index';
        this.doc_log_path = db_path + '/document.log';
        
        //document log, this is were all the docs are actually stored
        this.doc_log = new DocumentLog( this.doc_log_path );
    
        //TODO use defaults for now
        this.doc_index = new BPlusTree( this.doc_index_path );

        //TODO use defaults for now
        this.seq_index = new BPlusTree( this.seq_index_path );
    
        //walk the doc_index and update the doc_count
        //do everything else afterwards
        


        //TODO implement consistency checking and recovery        
    
        //TODO this is stuff for db_info requests, we should start tracking/providing it
        /* 
            Key                     Description
            db_name                 Name of the database (string)
            doc_count               Number of documents (including design documents) in the database (int)
            update_seq              Current number of updates to the database (int)
            purge_seq               Number of purge operations (int)
            compact_running         Indicates, if a compaction is running (boolean)
            disk_size               Current size in Bytes of the database (Note: Size of views indexes on disk are not included)
            instance_start_time     Timestamp of CouchDBs start time (int in ms)
            disk_format_version     Current version of the internal database format on disk (int)
        */

        this.compact_running =  false;
        this.disk_format_version =  1;        
        this.calculate_disk_size = function() { return 12345 }; 

        this.doc_del_count = 1;
        this.instance_start_time = new Date().getTime();
        this.purge_seq = 0;
        this.update_seq = 1;

        var on_docs_counted = function on_docs_counted( err, doc_count) {

            if (! err ) {
                that.doc_count = doc_count;
            }
        
            //nextTick ensures that on_ready doesn't get called until after
            //constructor finishes
            return process.nextTick( function() { on_ready( err ); } );
        };
        this._count_docs( on_docs_counted );

};
exports.DB = DB;


DB.prototype._count_docs = function _count_docs( on_docs_counted ) {

    var that = this;
    
    if ( this.doc_index.is_empty() ) {        
        return on_docs_counted( null,  0 );
    }
    
    var doc_count = 0;    
    var emit = function emit( doc_history ) {
    
        //check if the last entry in the doc history is deleted
        //if so then don't include it
        //TODO make a history object wrapper with a .JSON method for
        //serialization
        if ( doc_history[doc_history.length-1][4] === 1 ) {
            doc_count++;     
        }
        
    };
    
    var on_walk_complete = function on_walk_complete( err ) {
        
        if ( err ) {
            return on_docs_counted( err );
        }
        else {
            return on_docs_counted( null, doc_count );   
        }
        
    };
    
    this.doc_index.walk( emit, on_walk_complete );
    
};


/* Close the db. Any file descriptors associated with this db will also get closed 
    callback: on_closed( err )
*/
DB.prototype.close = function close( on_db_closed ) {

    var components_closed = 0;
    var total_components = 3;
    var on_component_closed = function on_component_closed( err ) {
        
        if ( err ) {
            return on_db_closed( err );   
        }
        else {
            components_closed++;   
        }
        
        if ( components_closed === total_components ) {
            
            return on_db_closed( null );    
        }
        
    };
    this.doc_log.close( on_component_closed );
    this.doc_index.close( on_component_closed );
    this.seq_index.close( on_component_closed );

};


/* Erase all files associated with this Database
    callback: on_closed( err )
*/
DB.prototype.erase = function erase( on_erased ) {

    var that = this;
    
    var on_db_closed = function on_db_closed( err ) {
        
        if ( err ) {
            on_erased( err );   
        }
        
        var total_components = 3;
        var components_erased = 0;
        var on_unlinked_component = function on_unlink_component( err ) {
        
            if ( err ) { 
                on_erased( err ) 
            };
    
            components_erased++;
        
            if ( components_erased === total_components ) {            
                on_erased( null );   
            }
        };
        fs.unlink( that.seq_index_path, on_unlinked_component );
        fs.unlink( that.doc_index_path, on_unlinked_component );
        fs.unlink( that.doc_log_path, on_unlinked_component );
    };
    this.close( on_db_closed );
};

/* Return the view object given a document id
*/
DB.prototype.get_view = function get_view( doc_id ) {
  
    //TODO implement caching, for now we retrieve the design document all the time
    
};

/* Create a temporary view. 
*/
DB.prototype.create_temp_view = function create_temp_view( map, reduce ) {
    //TODO implement
};


/* Return's some information about the DB. 
   See http://wiki.apache.org/couchdb/HTTP_database_API
*/
DB.prototype.info = function db_info() {
    //TODO implement        
};


/* Get all the document's from the DB. Every document is passed to emit once available.
   callbacks:
        emit( doc )
        on_complete( err ) 
*/
DB.prototype.all_docs = function all_docs( emit, on_complete ) {
    
        //walk document index db
};    


/* Add a document.
   callbacks:
        on_added( err, doc )

*/
DB.prototype.add_or_update_doc = function add_or_update_doc( doc, on_add_or_updated ) {

    //check for a doc_id
    //TODO doc_id sanity checking
    if ( ! doc._id ) {
       
       return on_add_or_updated( "doc id is required" );
        
    }
    
    if ( ! doc._rev ) { //first revision, insert [rev, reference, length] ] into the doc index
   
        this._add_doc( doc, on_add_or_updated );
    }
    else {
        this._update_doc( doc, on_add_or_updated );   
    }
        
};


DB.prototype._add_doc = function _add_doc(doc, on_added) {

    var that = this;
    //generate the revision
    doc._rev = '1-' + generate_uuid()

    //this get's used by closures to pass on the sequence number
    var global_seq, global_reference, global_length;

    //once we find the leaf node where this new document will be inserted the following
    //function will provide the value
    //TODO this matches how update works, but we could just next callbacks starting from
    //doc_log.append to seq_index.insert, perhaps this would be cleaner?
    var value_function = function value_function(context, continuation) {

            //first we attempt to write the document to the document log,
            var on_append_complete = function on_append_complete(err, seq, reference, length) {

                    if (err) {
                        return continuation(err);
                    }

                    //pass on the sequence number
                    global_seq = seq;
                    global_reference = reference;
                    global_length = length;

                    //if that succeeds we pass [rev, reference, length ] as the value to be
                    //inserted into the doc index                
                    var value = [ [seq, doc._rev, reference, length] ];
                    return continuation(null, value);
                };
            that.doc_log.append([doc._id, doc._rev, doc], on_append_complete);
        };

    //once the insertion into the document index is complete
    var on_inserted_doc_index = function on_inserted_doc_index(err) {

            if (err) {
                return on_added(err);
            }

            //these should be set by the value_function above if all went well
            var seq = global_seq,
                reference = global_reference,
                length = global_length;

            if ( !seq || !reference || !length) {

                return on_added('document log results not passed on?');
            }

            var on_inserted_seq_index = function on_inserted_seq_index(err) {

                    if (err) {

                        //TODO what do we do if the sequence number index insert falls but everything else
                        //has succeeded?                    
                        return on_added(err); //for now
                    }

                    return on_added(null, doc);
                };
                
            that.seq_index.insert(seq, [doc._id, doc._rev, reference, length], on_inserted_seq_index);

        };
    this.doc_index.insert(doc._id, value_function, on_inserted_doc_index );
};   


DB.prototype._update_doc = function _update_doc(doc, on_updated ) {

    var that = this;
    
    var global_last_seq;
    
        var update_rev = doc._rev;
        
        //once (if) we find where the document would go in the index...
        var value_function = function value_function( context, continuation ) {
            
            var doc_history = context.old_value;
            var last_update = doc_history[doc_history.length-1];
            var last_seq = last_update[0];
            var last_rev = last_update[1];            

            var last_rev_base = parseInt(last_rev.split('-')[0]);
            var update_rev_base = parseInt(update_rev.split('-')[0]);

            if ( last_rev_base === update_rev_base && last_rev !== update_rev ) {                
                
                //TODO implement conflict stuff                
                continuation( 'Document update conflict' );
            }
            else if (  last_rev !== update_rev ) { //base doesn't match either
                //TODO use the proper error string
                continuation( 'Bad revision' );
            }
            else {
                
                var new_rev_base = last_rev_base + 1;
                var new_rev = new_rev_base + '-' + generate_uuid();
                doc._rev = new_rev;
                
                //go append a new document to the document log
                 var on_append_complete = function on_append_complete(err, seq, reference, length) {

                    if (err) {
                        return continuation(err);
                    }

                    //pass on the sequence number
                    global_seq = seq;
                    global_reference = reference;
                    global_length = length;
                    global_last_seq = last_seq;

                    //if that succeeds we pass [rev, reference, length ] as the value to be
                    //inserted into the doc index 
                    doc_history.push( [seq, doc._rev, reference, length] );
                    return continuation(null, doc_history );
                };
                that.doc_log.append( [doc._id, doc._rev, doc], on_append_complete );
            }
            
        };
  
        //once the insertion into the document index is complete
        var on_doc_index_updated = function on_doc_index_updated(err) {

            if (err) {
                //TODO doc was appened but index wasn't updated... what now?
                return on_updated(err);
            }

            //these should be set by the value_function above if all went well
            var seq = global_seq,
                reference = global_reference,
                length = global_length,
                last_seq = global_last_seq;

            if ( !seq || !reference || !length) {

                return on_added('document log results not passed on?');
            }

            var on_inserted_seq_index = function on_inserted_seq_index( err ) {

                    if (err) {

                        //TODO what do we do if the sequence number index insert falls but everything else
                        //has succeeded?                    
                        return on_updated(err); //for now
                    }

                    //now we need to delete the last_sequence number from the tree 
                    //so that walking it gives us a changes feed
                    on_deleted_seq_index = function on_deleted_seq_index( err ) {

                        if (err) {

                            //TODO what do we do if the sequence number index insert falls but everything else
                            //has succeeded?                    
                            return on_updated(err); //for now
                        }

                        return on_updated(null, doc);    
                    };
                    that.seq_index.delete( last_seq,  on_deleted_seq_index );                                    

                    
            };

            that.seq_index.insert(seq, [doc._id, doc._rev, reference, length], on_inserted_seq_index);

    };
    this.doc_index.update(doc._id, value_function, on_doc_index_updated );
        
};



/* Retrieve a specific document by id from the DB. 
   callbacks:
        on_retrieved( err, doc )

*/
DB.prototype.retrieve_doc = function retrieve_doc( doc_id, rev, on_doc_retrieved ) {
    
    var that = this;
    
    //once the history is retrieved for this document
    var on_value_retrieved = function on_value_retrieved( err, value ) {
                
        if ( err ) {
            
            return on_doc_retrieved( 'missing' );       
        }

        var history = value;
        
        var last_update = history[history.length - 1];
        var last_revision = last_update[1];
        var reference = last_update[2];
        var length = last_update[3];                        
        var deleted = last_update[4]; //set optionaly by deletes
        
        //specific revision
        if ( rev ) {
            
            //check the last revision, see if this document as been deleted
            
            //TODO optimize somehow?
            var rev_found = false;
            for ( var i = 0; i < history.length; i++ ) {
                var item = history[i];
                if ( rev == item[1] ) { //found revision!
                    //once we get it from the doc log sent it up
                    var reference = item[2];
                    var length = item[3];                    
                    var item_deleted = item[4];
                    
                    //no need to retrieve it from doc log, we have all we need?
                    if ( item_deleted  ) {
                        return on_doc_retrieved( null, { _id : doc_id, _rev : rev, _deleted : true  } )
                    }
                    
                    var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
                            
                        if ( err ) {                            
                            return on_doc_retrieved( err );
                        }
                        var doc = log_item[2];
                        return on_doc_retrieved( null, doc );                        
                    };
                    that.doc_log.retrieve( reference, length, on_log_item_retrieved ); 
                    rev_found = true;
                    break;
                   
                   
                }       
            }
        
            //if we got here, the revision isn't in the history
            if ( ! rev_found ) {
                return on_doc_retrieved( 'missing' );       
            }
        }
        else { //last item in history
            
            //with a revision, we return nothing if the doc has been deleted
            if ( deleted ) {
                return on_doc_retrieved( 'deleted' );   
            }
                            
            var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
                            
                if ( err ) {                            
                    return on_doc_retrieved( err );
                }
                var doc = log_item[2];
                return on_doc_retrieved( null, doc );                        
            };
            that.doc_log.retrieve( reference, length, on_log_item_retrieved );
        }
                
    };
    this.doc_index.retrieve( doc_id, on_value_retrieved );

};    


/* Delete a document. 
   callbacks:
        on_deleted( err, new_doc_id )

*/
DB.prototype.delete_doc = function delete_doc( doc_id, doc_rev, on_deleted ) {
    
    var that = this;

    //this get's used by closures to pass on the sequence numbers etc
    var global_seq, global_reference, global_length, global_new_doc, global_last_seq;
     
    //deleting really means removing the last revision and then added a new 'deleted' revision
    var value_function = function value_function( context, continuation ) {
        
        var doc_history = context.old_value;
        var last_update = doc_history[doc_history.length-1];
        var last_rev = last_update[1];
        var last_seq = last_update[0];
        
        if ( doc_rev !== last_rev ) {
             continuation( 'Bad revision' );
        }
        else {
                
            var new_rev_base = parseInt(last_rev.split('-')[0]) + 1;            
            var new_rev = new_rev_base + '-' + generate_uuid();
            var new_doc = { _rev : new_rev, _id : doc_id, _deleted : true };
            
            //pass to other closures
            global_new_doc = new_doc;

            //go append a new document to the document log
            var on_append_complete = function on_append_complete(err, seq, reference, length) {

                    if (err) {
                        return continuation(err);
                    }

                    //pass on the sequence number
                    global_seq = seq;
                    global_reference = reference;
                    global_length = length;
                    global_last_seq = last_seq;

                    //if that succeeds we pass [rev, reference, length ] as the value to be
                    //inserted into the doc index 
                    var deleted = 1;
                    doc_history.push( [seq, new_doc._rev, reference, length, deleted ] );
                    return continuation(null, doc_history );
                
            };
            that.doc_log.append( [new_doc._id, new_doc._rev, new_doc], on_append_complete );
            
        }
    };        

    var on_doc_index_updated = function on_doc_index_updated( err, history ) {            
        
            var new_doc = global_new_doc;

            if (err) {
                //TODO doc was appened but index wasn't updated... what now?
                return on_updated(err);
            }

            //these should be set by the value_function above if all went well
            var seq = global_seq,
                reference = global_reference,
                length = global_length,
                last_seq = global_last_seq;

            if ( !seq || !reference || !length) {

                return on_deleted('document log results not passed on?');
            }

            var on_inserted_seq_index = function on_inserted_seq_index(err) {

                    if (err) {
                        //TODO what do we do if the sequence number index insert falls but everything else
                        //has succeeded?                    
                        return on_deleted(err); //for now
                    }

                    //now we need to delete the last_sequence number from the tree 
                    //so that walking it gives us a changes feed
                    on_deleted_seq_index = function on_deleted_seq_index( err ) {

                        if (err) {
                            //TODO what do we do if the sequence number index insert falls but everything else
                            //has succeeded?                    
                            return on_deleted(err); //for now
                        }

                        return on_deleted(null, new_doc._rev );    
                    };
                    that.seq_index.delete( last_seq,  on_deleted_seq_index );
            };
            
            var deleted = 1; //for changes feed
            that.seq_index.insert(seq, [new_doc._id, new_doc._rev, reference, length, deleted], on_inserted_seq_index);
    };
    
    this.doc_index.update(doc_id, value_function, on_doc_index_updated );

        

};

