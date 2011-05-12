var BPlusTree = require('../lib/bplus_tree').BPlusTree;
var DocumentLog = require('../lib/document_log').DocumentLog;

/* DB Object constructor.
*/
var DB = function DB( db_path, create_if_missing ) {
        
        if ( ! db_path ) {
            throw "db_path is a required param";   
        }
        
        //TODo check the path exists        
        //TODO shoud we create if missing?        
        var path_split = db_path.split( '/' );
        
        this.name = path_split.pop;
        this.doc_index_path = db_path + '/document.index'; 
        this.doc_log_path = db_path + '/document.log';

        //document log, this is were all the docs are actually stored
        var doc_log = DocumentLog( this.document_log_path );
    
        //use defaults for now
        var doc_index = BPlusTree( this.document_index_path );

        //check that the last sequence number in the document index
        //matches the last sequence number in the doc log
        
        if ( doc_log.last_sequence_number != doc_index.last_sequence_index ) {
            //TODO implement recovery 
            throw "doc_log and doc_index are inconsistent and recovery isn't implemented";
        }

        this.doc_log = doc_log;
        this.doc_index = doc_index;
        
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
    //TODO implement       
};    


/* Add a document.
   callbacks:
        on_added( err, doc )

*/
DB.prototype.add_doc = function add_doc( doc, on_added ) {
    //TODO implement       
};

/* Retrieve a specific document by id from the DB. 
   callbacks:
        on_retrieved( err, doc )

*/
DB.prototype.retrieve_doc = function retrieve_doc( doc_id, revision, on_retrieved ) {
    //TODO implement       
};    


/* Update a document. 
   callbacks:
        on_updated( err )

*/
DB.prototype.update_doc = function update_doc( doc_id, revision, on_updated ) {
    //TODO implement       
};

/* Delete a document. 
   callbacks:
        on_updated( err )

*/
DB.prototype.delete_doc = function delete_doc( doc_id, revision, on_updated ) {
    //TODO implement       
};

