var util = require("util");
var events = require("events");
var path = require('path');
var fs = require('fs');
/*
*/
var AttachmentStore = function AttachmentStore( store_path, on_ready ) {
        
    this.store_path = store_path;
    
    //asynchronously check the the path exists, then we can create it (or not )
    var on_path_check_complete = function on_path_check_complete( exists ) {
        
        if ( exists ) {
            //...

            return on_ready( null );
        }

        
        //once the dir has been created... 
        var on_path_created = function on_path_created( err ) {
        
            if ( err ) {
                return on_ready( err );                   
            }
            
            return on_ready( null );                        
        }
        fs.mkdir( store_path, '0755', on_path_created );        
    };
    path.exists( store_path, on_path_check_complete );

};
exports.AttachmentStore = AttachmentStore;


/* Stores the file associated with tmp_file_name into the attachment under the given id and file_name
    callback: on_stored( err )
*/
AttachmentStore.prototype.store = function store( id, file_name, tmp_file_name, on_stored ) {
    
    //TODO revisit implementation. For now this is really simple. The id is a
    //sub dir and file_name is a file in it
  
    var attachment_dir = this.store_path + '/' + id;
    
    var on_path_check_complete = function on_path_check_complete( exists ) {

        var attachment_file_path = attachment_dir + '/' + file_name;
    
        //once the dir has been created... 
        var on_path_created = function on_path_created( err ) {
        
            if ( err ) {
                return on_stored( err );                
            }

            on_moved = function on_moved( err ) {
                
                return on_stored( err );
            };
            fs.rename( tmp_file_name, attachment_file_path, on_moved );
        }

        if ( ! exists ) {
                
            return fs.mkdir( attachment_dir, '0755', on_path_created );
        }
        else {
            return on_path_created( null );    
        }

    };
    path.exists( attachment_dir, on_path_check_complete );
        
};


/* Retrieves the readable stream associated with id and filename 
    callback: on_stream_ready( err, readable_stream )
*/
AttachmentStore.prototype.retrieve_attachment_stream = function retrieve_attachment_stream( id, file_name, on_stream_ready ) {
    
    //TODO revisit implementation. For now this is really simple. The id is a
    //sub dir and file_name is a file in it
  
    var attachment_dir = this.store_path + '/' + id;
    var attachment_file_path = attachment_dir + '/' + file_name;
    
    
    var on_stats = function on_stats( err, stats ) {

        if ( err ) { return on_stream_ready( err ); }

        if ( ! stats.isFile() ) { return on_stream_ready( 'not a file?' ); }
        
        var attachment_stream = fs.createReadStream( attachment_file_path );
        return on_stream_ready( null, attachment_stream  );
    };
    fs.stat( attachment_file_path, on_stats )
};


