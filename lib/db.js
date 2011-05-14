var BPlusTree = require('../lib/bplus_tree').BPlusTree;
var DocumentLog = require('../lib/document_log').DocumentLog;
var generate_uuid = require('node-uuid');

/* DB Object constructor.
*/
var DB = function DB( db_path, create_if_missing ) {
        
        if ( ! db_path ) {
            throw "db_path is a required param";   
        }
        
        //TODO check the path exists        
        //TODO shoud we create if missing?        
        var path_split = db_path.split( '/' );
        
        this.name = path_split.pop;
        this.doc_index_path = db_path + '/document.index'; 
        this.sequence_number_index_path = db_path + '/sequence_number.index';
        this.doc_log_path = db_path + '/document.log';
        
        //document log, this is were all the docs are actually stored
        this.doc_log = DocumentLog( this.document_log_path );
    
        //TODO use defaults for now
        this.doc_index = BPlusTree( this.document_index_path );

        //TODO use defaults for now
        var seq_index = BPlusTree( this.sequence_number_index_path );
    
        //check that the last sequence number in the document index
        //matches the last sequence number in the doc log
        
        if ( this.doc_log.sequence_number != this.doc_index.sequence_number ) {
            //TODO implement recovery 
            throw "doc_log and doc_index are inconsistent and recovery isn't implemented";
        }

        
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
        this.doc_count = 1;
        this.doc_del_count = 1;
        this.instance_start_time = new Date().getTime();
        this.purge_seq = 0;
        this.update_seq = 1;
    
};
exports.DB = DB;


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
DB.prototype.add_or_update_doc = function add_doc( doc, on_add_or_updated ) {

    //check for a doc_id
    //TODO doc_id sanity checking
    if ( ! doc._id ) {
        doc._id = generate_uuid();    
    }
    
    if ( ! doc._rev ) { //first revision, insert [rev, reference, length] ] into the doc index
   
        this._add_doc( doc, on_add_or_updated );
    }
    else {
        this.update_doc( doc, on_add_or_updated );   
    }
        
};


DB.prototype._add_doc = function _add_doc(doc, on_added) {

    //generate the revision
    doc._rev = '1-' + generate_uuid()

    //this get's used by closures to pass on the sequence number
    var global_seq, global_reference, global_length;

    //once we find the leaf node where this new document will be inserted the following
    //function will provide the value
    //TODO this matches how update works, but we could just next callbacks starting from
    //document_log.append to seq_index.insert, perhaps this would be cleaner?
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
                    return continuation(null, [ [seq, rev, reference, length] ]);
                };
            that.document_log.append([doc._id, rev, doc], on_append_complete);
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
            that.seq_index.insert(seq, [doc._id, rev, reference, length]);

        };
    that.doc_index.insert(doc._id, value_function );
};   


DB.prototype._update_doc = function _update_doc(doc, on_updated ) {

        var update_rev = doc._rev;
        
        //once (if) we find where the document would go in the index...
        var value_function = function value_function( context, continuation ) {
            
            var doc_history = context.value;
            var last_update = doc_history[doc_history.length-1];
            var last_rev = last_update[1];            
        
            if ( last_rev == update_rev ) {                
                continuation( 'Document update conflict' );
            }
            else {
                
                //go append a new document to the document log
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
                    doc_history.push( [seq, rev, reference, length] );
                    return continuation(null, doc_history );
                };
                that.document_log.append( [doc._id, rev, doc], on_append_complete );
            }
            
        };
  
   //once the insertion into the document index is complete
    var on_inserted_doc_index = function on_inserted_doc_index(err) {

            if (err) {
                //TODO doc was appened but index wasn't updated... what now?
                return on_updated(err);
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
            that.seq_index.insert(seq, [doc._id, rev, reference, length]);

    };
    that.doc_index.insert(doc._id, value_function );
        
};



/* Retrieve a specific document by id from the DB. 
   callbacks:
        on_retrieved( err, doc )

*/
DB.prototype.retrieve_doc = function retrieve_doc( doc_id, rev, on_doc_retrieved ) {
    
    //once the history is retrieved for this document
    var on_value_retrieved = function on_value_retrieved( err, value ) {
        
        var history = value;
        
        if ( err ) {
            
            return on_doc_retrieved( 'missing' );       
        }
         
        //specific revision
        if ( rev ) {
            
            //TODO optimize somehow?
            for ( var i = 0; i < history.length; i++ ) {
                var item = history[i];
                if ( rev == item[1] ) { //found revision!
                    //once we get it from the doc log sent it up
                    var reference = item[2];
                    var length = item[3];
                    
                    var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
                            
                        if ( err ) {                            
                            return on_doc_retrieved( err );
                        }
                        var doc = log_item[2];
                        return on_doc_retrieved( null, doc );                        
                    };
                    this.doc_log.retrieve( reference, length, on_log_item_retrieved ); 
                }       
            }
        
            //if we got here, the revision isn't in the history
            return on_doc_retrieved( 'missing' );       
        }
        else { //last item in history
            
            var item = history[history.length - 1];
            var on_log_item_retrieved = function on_log_item_retrieved( err, log_item ) {
                            
                if ( err ) {                            
                    return on_doc_retrieved( err );
                }
                var doc = log_item[2];
                return on_doc_retrieved( null, doc );                        
            };
            this.doc_log.retrieve( reference, length, on_log_item_retrieved );
        }
                
    };
    this.doc_index.retrieve( doc_id, on_value_retrieved );

};    


/* Delete a document. 
   callbacks:
        on_updated( err )

*/
DB.prototype.delete_doc = function delete_doc( doc_id, revision, on_updated ) {
    
    //append the update stub into the docment log, we'll rollback later if something goes wrong
    
    //define our value confirmation callback to check the revision
        //if the revision is good return true
        //else false and provide error
    
    //if deleted successfully insert sequence number into
        
    
};

