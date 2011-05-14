var DB = require('./database').DB;
var path = require('path');
var fs = require('fs');

//TODO implement
var DBManager = function DBManager( db_path ) {
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
    
    for (var i = 0; i < entries.length; i++ ) {
        name = entries[i];
        var full_path = this.db_root + '/' + name;
        var stats = fs.statSync(  full_path );
        if ( stats.isDirectory() ) {
            
            //TODO some sort of sanity checks?
            var db = new DB( full_path, false );
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
            
            var db = new DB( new_db_path, true );
            that.db_by_name[name] = db;
            return on_created( null, db );
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
        
        //TODO check the filesystem even if it's not in the cache?
        
};


/* Delete the db identified by name.
   callback:
        on_deleted( err ) 
*/
DBManager.prototype.delete_db = function delete_db(name, on_deleted) {

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
                                        delete this.db_by_name[name];
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
};