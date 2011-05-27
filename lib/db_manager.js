var DB = require('./database').DB;
var path = require('path');
var fs = require('fs');
var util = require('util');


/* DBManager's provide access to DB objects. This is the Async constructor.
    callback: on_ready( err, dm )
*/
var DBManager = function DBManager( db_path, on_ready ) {

    if ( ! db_path ) {
        throw 'db_path is a required parameter';           
    }
    
    if ( ! path.existsSync( db_path ) ) {
        throw db_path + " does not exist";
    }
    this.db_root = db_path;

    this.db_by_name = {};
    
    //scour the path looking for dbs?
    var entries = fs.readdirSync(db_path);
    var total_dbs = 0;
    for (var i = 0; i < entries.length; i++ ) {
        name = entries[i];
        var full_path = this.db_root + '/' + name;
        var stats = fs.statSync(  full_path );
        if ( stats.isDirectory() ) {
            total_dbs++;  
        }
    }
    
     //this get's updated in for loop below, hacky but it's very JSish
    var dbs_ready = 0;
    var on_db_constructed = function on_db_constructed( err, db ) {
        
        if ( err ) {
            return on_ready( err );   
        }
        
        dbs_ready++;
        
        if ( dbs_ready === total_dbs )
        {
            return on_ready( null );            
        }
    };
    
    for (var i = 0; i < entries.length; i++ ) {
        name = entries[i];
        var full_path = this.db_root + '/' + name;
        var stats = fs.statSync(  full_path );
        if ( stats.isDirectory() ) {
            //TODO some sort of sanity checks?
            var db = new DB( full_path, false, on_db_constructed );
            this.db_by_name[name] = db;
        }
    }
    

};
exports.DBManager = DBManager;


/* Create's a new database.
   callback:
        on_created( err, db ) 
*/
DBManager.prototype.create_db = function create_db( name, on_created ) {
    
    var that = this;
    
    var new_db_path = this.db_root + '/' + name;
    
    //asynchronously check the the path exists, then we can create it (or not )
    var on_path_check_complete = function on_path_check_complete( exists ) {
        
        if ( exists ) {
            return on_created( 'DB appears to already exist' );            
        }
         
        //once the dir has been created... 
        var on_path_created = function on_path_created( err ) {
        
            if ( err ) {
                return on_created( err );                   
            }
            
            var on_db_ready = function on_db_ready( err ) {

                if ( err ) {
                    delete that.db_by_name[name];   
                    return on_created( err );
                }

                return on_created( null, db );
            };
            
            var db = new DB( new_db_path, true, on_db_ready );
            that.db_by_name[name] = db;
                        
        }
        fs.mkdir( new_db_path, '0755', on_path_created );        
    };
    path.exists( new_db_path, on_path_check_complete );
};


/* Retrieve the DB objected identified by name.
   callbacks:
        on_retrieved( err, db )
*/
DBManager.prototype.retrieve_db = function retrieve_db( name, on_retrieved ) {
    
    var db = this.db_by_name[name]
    if ( db ) {
        return on_retrieved( null, db );
    }        
    else {
        return on_retrieved( 'file_not_found' );   
    }
        
};


/* Delete the db identified by name.
   callback:
        on_deleted( err ) 
*/
DBManager.prototype.delete_db = function delete_db(name, on_deleted) {

    var that = this;
    
    var db_path = this.db_root + '/' + name;
    var db = this.db_by_name[name];

    if (db) {
        var on_path_checked = function on_path_checked(exists) {
                if (exists) {

                    var on_db_erased = function on_db_erased(err) {

                            if (err) {
                                return on_deleted(err);
                            }

                            var on_rmdir = function on_rmdir(err) {

                                    if (err) { //and error could indicate something has a fd referencing something inside
                                        return on_deleted(err);
                                    }
                                    else {
                                        delete that.db_by_name[name];
                                        return on_deleted(null);
                                    }
                                };
                            fs.rmdir(db_path, on_rmdir);

                        };
                    db.erase(on_db_erased);

                }
                else { // path does not exist, wtf?
                    return on_deleted('DB path does not exist despite being in cache');
                }
            };
        path.exists(db_path, on_path_checked);
    }
    else {
        return on_deleted( 'not_found' );   
        
    }
    
};