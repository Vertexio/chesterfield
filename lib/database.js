var BPlusTree = require('../lib/bplus_tree').BPlusTree;
var DocumentLog = require('../lib/document_log').DocumentLog;
var AttachmentStore = require('../lib/attachments').AttachmentStore;

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
        
        
        //used to check that all outstanding callbacks have fired
        var outstanding_callbacks = 0;
        var err_in_progress = false;    
        var completion_checker = function completion_checker( err ) {      
            
            outstanding_callbacks--;
            if ( ! err_in_progress && ( err || outstanding_callbacks === 0 ) ) {        
                
                if ( err ) { err_in_progress = true };
                
                //nextTick ensures that on_ready doesn't get called until after
                //constructor finishes
                return process.nextTick( function() { on_ready( err ); } );
            }
        };
        
        //TODO check the path exists        
        //TODO shoud we create if missing?        
        var path_split = db_path.split( '/' );
        
        this.name = path_split.pop;
        this.doc_index_path = db_path + '/document.index'; 
        this.seq_index_path = db_path + '/sequence_number.index';
        this.doc_log_path = db_path + '/document.log';
        this.attachment_path = db_path + '/attachments';
        //document log, this is were all the docs are actually stored
        this.doc_log = new DocumentLog( this.doc_log_path );
    
        //TODO use defaults for now, make constructor async?
        this.doc_index = new BPlusTree( this.doc_index_path );

        //TODO use defaults for now, make constructor async?
        this.seq_index = new BPlusTree( this.seq_index_path );

        outstanding_callbacks++;
        var on_attachment_store_ready = function on_attachment_store_ready( err ) {
          
            return completion_checker( err );
        };
        this.attachment_store = new AttachmentStore( this.attachment_path, on_attachment_store_ready );
    
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
            return completion_checker();
            
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
    var emit = function emit( key, value ) {
    
        var doc_history = new DocHistory( value );
        
        //check if the last entry in the doc history is deleted
        //if so then don't include it
        //TODO change .deleted to be boolean?
        if ( doc_history[doc_history.length-1].deleted !== 1 ) {
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
                    var doc_history = new DocHistory();
                    doc_history.push( new DocHistoryItem( seq, doc._rev, reference, length ) );
                    return continuation(null, doc_history );
                };
            that.doc_log.append([doc._id, doc._rev, doc], on_append_complete);
        };

    //once the insertion into the document index is complete
    var on_inserted_doc_index = function on_inserted_doc_index(err) {

            if (err) {
                return on_added(err);
            }

            //update the doc count
            that.doc_count++;
            
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
        
        var doc_history = new DocHistory( context.old_value );
        var last_history_item = doc_history[doc_history.length-1];
        var last_seq = last_history_item.seq;
        var last_rev = last_history_item.rev            
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
                var doc_history = new DocHistory();
                doc_history.push( new DocHistoryItem(seq, doc._rev, reference, length) );
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


/* Retrieve a specific document's history by id from the DB. 
   callbacks:
        on_retrieved( err, doc_history )

*/
DB.prototype.retrieve_doc_history = function retrieve_doc_history( doc_id, on_doc_history_retrieved ) {

    
     //once the history is retrieved for this document
     var on_value_retrieved = function on_value_retrieved( err, value ) {
                
        if ( err ) {
            
            return on_doc_history_retrieved( 'missing' );       
        }

        var doc_history = new DocHistory( value );    
        return on_doc_history_retrieved( null, doc_history );

     };
     this.doc_index.retrieve( doc_id, on_value_retrieved );
};


/* Retrieve a specific document by id from the DB. 
   callbacks:
        on_retrieved( err, doc )

*/
DB.prototype.retrieve_doc = function retrieve_doc( doc_id, rev, on_doc_retrieved ) {
    
    var that = this;
    
    //once the history is retrieved for this document
    var on_doc_history_retrieved = function on_doc_history_retrieved( err, value ) {
                
        if ( err ) {
            
            return on_doc_history_retrieved( err );       
        }
        
        var last_history_item = doc_history[history.length - 1];
        var last_revision = last_history_item.rev;
        var reference = last_history_item.reference
        var length = last_history_item.len;                        
        var deleted = last_history_item.deleted; //set optionaly by deletes
        
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
    this.retrieve_doc_history( doc_id, on_doc_history_retrieved );

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
        
        var doc_history = new DocHistory( context.old_value );
        var last_history_item = doc_history[doc_history.length-1];
        var last_rev = last_history_item.rev;
        var last_seq = last_history_item.seq;
        
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
            
            //document was deleted, decrement the doc_count
            that.doc_count--;
            

            //these should be set by the value_function above if all went well
            var seq = global_seq,
                reference = global_reference,
                length = global_length,
                last_seq = global_last_seq;

            if ( ! seq || ! reference || ! length) {

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


/* Walk the document histories.
   callbacks:
        emit( doc_history )
        on_complete( err )
*/
DB.prototype.walk_doc_histories = function walk_doc_histories( emit, on_complete, from_a, to_z ) {
    
    var that = this;
    
    if ( this.doc_count === 0 ) {        
        return on_complete( null );
    }
    
    //the B+Tree values are document histories in json form
    var emit_value = function emit_value( key, value ) {
    
        var doc_history = new DocHistory( value );
        
        emit( doc_history );
    };
    
    var on_btree_walk_complete = function on_btree_walk_complete( err ) {
        
        if ( err ) {
            return on_complete( err );
        }
        else {
            return on_complete( null );   
        }
        
    };
    
    this.doc_index.walk( emit_value, on_btree_walk_complete, from_a, to_z );
};


/* Walk the most recent non-deleted documents
   callbacks:
        emit( doc )
        on_complete( err )
*/
DB.prototype.walk_docs = function walk_docs( emit, on_complete, from_a, to_z  ) {

    var that = this;
    
    //problem: on_complete will likely fire before the results of some or all of
    //of these retrievals
    //solution: keep track of the total docs to be emitted only return complete
    //afterwards
    //TODO this might need to be reworked, the log retrieval kind of breaks
    //this model

    var to_be_emitted = 0;
    var emitted_so_far = 0;
    var on_complete_wating = false;
    
    var completion_checker = function completion_checker() {      

        //make sure on_complete is waiting, otherwise there may be still
        //more emit's to come even though emitted_so_far === to_be_emitted
        if ( on_complete_waiting && emitted_so_far === to_be_emitted ) {        
            return on_complete( null );
        }
    };

    var emit_doc_history = function emit_doc_history( doc_history ) {
                
        var doc_history_item = doc_history[doc_history.length-1];
        if ( doc_history_item.deleted !== 1 ) {
            
            to_be_emitted++;
            var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
                            
                if ( err ) {                            
                    return on_complete( err );
                }
                else { 
                    //TODO write a wrapper around log_item to hide array impl
                    var doc = log_item[2];
                    emit( doc );                        
                    emitted_so_far++;
                    completion_checker();
                }
            };
            that.doc_log.retrieve( doc_history_item.reference, 
                                   doc_history_item.len, on_log_item_retrieved );
        }
    };

    var on_history_walk_complete = function on_history_walk_complete( err ) {
      
        if ( err ) {
            return on_complete( err );   
        }

        on_complete_waiting = true;
        completion_checker();
    };  
    
    this.walk_doc_histories( emit_doc_history, on_history_walk_complete, from_a, to_z );
};


/* Used to attach a file to a document given the id and revision. 
    callbacks: on_stream_ready( err, attachment_stream )
*/
DB.prototype.attach = function attach( doc_id, rev, file_name, tmp_file_name, on_attached ) {
    
    var that = this;
    
    //get doc history, check the revision
    var on_doc_history_retrieved = function on_doc_history_retrieved( err, doc_history ) {

        if ( err ) { return on_attached( err ); };

        var last_item = doc_history[doc_history.length-1];
        
        //TODO support for creating a new doc and attaching file when doc doesn't exist
        if ( last_item.rev !== rev ) {
            
            //TODO delete the temp file
            return on_attached( 'bad revision' ); //TODO use right error msg
        }

        var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
          
            if ( err ) { return on_attached( err ); };
          
            var doc = log_item[2];
            
            var on_stored = function on_stored( err ) {

                //update the doc
                //once this document has been updated this attachment is complete
                that._update_doc( doc, on_attached );      
            };
            that.attachment_store.store( doc_id, file_name, tmp_file_name, on_stored );
                                        
        };            
        that.doc_log.retrieve( last_item.reference, last_item.len, on_log_item_retrieved );                           
    };
    this.retrieve_doc_history( doc_id, on_doc_history_retrieved );    
};


/* DocHistory is a convenience abstraction around the JSON which 
   specifiecs a Document History Item array in the document index B+Tree.
*/
var DocHistory = function DocHistory( json ) {
  
    if ( json ) {
        
        if ( ! json instanceof Array ) {
            throw "json should be array";   
        }
    
        for ( var i = 0; i < json.length; i++ ) {
            this.push( new DocHistoryItem_from_json( json[i] ) );   
        }       
        
    }
};
exports.DocHistory = DocHistory;

//make DocHistory subclasses of Array
DocHistory.prototype = new Array();


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



