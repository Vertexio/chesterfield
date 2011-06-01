/*
   Copyright 2011 pcapr
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


var DB = require('./database').DB;

this.mount = function(app, dbm) {

    var status = {
        'illegal_database_name': 400,
        'bad_request': 400,
        'not_found': 404,
        'file_exists': 412,
        'internal_server_error': 500
    };
    

    //database creation
    app.put('/:db', function(req, res) {
        
        var name = req.params.db;       
        
        var on_db_created = function on_db_created( err ) {

            var json;
            if ( err ) {
                //TODO there may be another reason why this is failing
                json = { ok : false, error : 'file_exists' };
            }
            else {
                json = { ok : true };             
            }
            
            var headers = {};
            headers['Location'] = 'http://' + req.headers.host + '/' + name;
            res.json(json, status[json.error] || 201, headers);                
        };        
       
        dbm.create_db( name, on_db_created );

    });


    //get all databases      
    app.get('/_all_dbs', function(req, res) {
        
    
        //TODO for now we'll just put all the db name into an array, eventually we may avoid this to reduce memory        
        var dbs = [];
        var emit = function emit( db ) {

            dbs.push(db);
        };    
        var on_complete = function on_complete( err ) {            

            if ( err ) {
                json = { error: 'internal_server_error' };
            }
            else {
                json = dbs;
            }
            
            res.json(json, status[json.error] || 200 );
        };
        dbm.walk_dbs( emit, on_complete );
       
    });
    
    
    //TODO implement replication
    app.post('/_replicate', function(req, res) {
        res.niy();
    });


    //db info request
    app.get('/:db', function(req, res) {

        var name = req.params.db;
                
        var on_retrieved = function on_retrieved( err, db ) {
          
            var json;
            if ( err ) {
             
                json = { error : 'not_found' };    
            }
            else {
             
                json  = {
                    "compact_running": db.compact_running, 
                    "db_name": name, 
                    "disk_format_version": db.disk_format_version, 
                    "disk_size": db.calculate_disk_size(), 
                    "doc_count": db.doc_count, 
                    "doc_del_count": db.del_count, 
                    "instance_start_time": db.instance_start_time, 
                    "purge_seq": db.purge_seq, 
                    "update_seq": db.update_seq
                };   
            }  
            res.json(json, status[json.error] || 200);
        };
        dbm.retrieve_db( name, on_retrieved );
        
    });

    //db delete
    app.del('/:db', function(req, res) {
        
        var name = req.params.db;
        
        if (req.query.rev) {

            res.json({ error: 'bad_request', reason: 'You tried to DELETE a database with a ?=rev parameter. Did you mean to DELETE a document instead?' }, 400);
            res.json(json, status[json.error] || 200);
        } 
        else {    
        
            var on_deleted = function on_deleted( err ) {    
    
                var json = {};
                if ( err ) {
        
                    json.error = 'not_found';
                }
                else {
                    
                    json.ok = true;
                }
                res.json(json, status[json.error] || 200);
            };
            dbm.delete_db( name, on_deleted );            
        } 
    }); 


    //ensure a full commit, chesterfield changes are always durable, this is superflous
    app.post('/:db/_ensure_full_commit', function(req, res) {
        
        var name = req.params.db;
        
        var on_retrieved = function on_retrieved( err, db ) {
            var json;
            if ( err ) {
             
                json = { error : 'not_found' }; 
            }
            else {
                
                json = { ok: true, instance_start_time: db.instance_start_time.toString() }
            }
            res.json(json, status[json.error] || 200);
        };
        dbm.retrieve_db( name, on_retrieved );
    });
    
       //_all docs
    app.get('/:db/_all_docs', function(req, res) {

        var db_name = req.params.db;
        var include_docs = req.query.include_docs && eq.query.include_docs.toLowerCase() === 'true' ? true : false;
        
        //TODO support for these and other params
        var from_a;
        var to_z;
        
        var error_handler = function error_handler( err ) {
            
            //TODO everything is an internal server error for now but should
            //be done properly eventually
            var json = { error : 'internal_server_error', reason: err };
            return res.json(json, status[json.error] );            
        }
        
        var on_db_retrieved = function on_db_retrieved( err, db ) {
            
            if ( err ) {
                
                return error_handler( err );
            }
                        
            var total_rows = 0;
            var offset = 0;
            
            if ( db.doc_count === 0 ) {
                
                return res.json( {"total_rows":0,"offset":0,"rows":[]}, 200  ) ;
            }

            //first walk the document histories to get the document count
            //excluding deleted docs
            var doc_count = 0; 
            var emit_doc_history = function emit_doc_history( doc_history ) {
             
                var last_item = doc_history.get_current();
                if ( last_item.deleted !== 1 ) {
                    doc_count++;
                }
            };
            
            var on_history_walk_complete = function on_history_walk_complete( err ) {
              
                if ( err ) {
                  
                    return error_handler( err );
                }    

                //manually generate the start of the JSON response
                var headers = { 'Content-Type' : 'application/json' };
                res.writeHead(status || 200, headers);
                res.write( '{\n\t"total_rows":' + doc_count +',' + '"offset:0,' +
                           '"rows" : [' );

                var is_first = true;
                //walk the actual docs now, as docs get emitted stream them
                var emit_doc = function emit_doc( doc ) {
              
                    doc_info = { id : doc._id, key : doc._id, value : { rev : doc._rev} };
                    
                    if ( include_docs ) 
                    {
                        doc_info.doc = doc;
                    }
                    
                    var json_string = JSON.stringify( doc_info );
                    
                    if ( is_first ) {
                        res.write( '\n\t\t' + json_string );
                        is_first = false;
                    }
                    else {
                        res.write( ',\n\t\t' + json_string );
                    }
                          
                };
                
                //on complete, close of the message
                var on_doc_walk_complete = function on_doc_walk_complete( err ) {
                    
                    if ( err ) {
                        return error_handler( err );    
                    }
                    
                    res.end('\n\t]\n}\n')
                };    
                
                db.walk_docs( emit_doc, on_doc_walk_complete, from_a, to_z );
            };
            db.walk_doc_histories( emit_doc_history, on_history_walk_complete, from_a, to_z );
        };
        dbm.retrieve_db( db_name, on_db_retrieved );        
    });

    
      //_changes feed
    app.get( '/:db_name/_changes', function( req, res ) {
        
        var db_name = req.params.db_name;
        var include_docs = req.query.include_docs && req.query.include_docs.toLowerCase() === 'true' ? true : false;
        var continous = req.query.feed && req.query.feed.toLowerCase() === 'continous' ? true : false;
        var longpoll = req.query.feed && req.query.feed.toLowerCase() === 'longpoll' ? true : false;
        var heartbeat = req.query.heartbeat ? parseInt( req.query.heartbeat ) : null;
        
        var error_handler = function error_handler( err ) {
            
            //TODO everything is an internal server error for now but should
            //be done properly eventually
            var json = { error : 'internal_server_error', reason: err };
            return res.json(json, status[json.error] );            
        }
        

        
        var is_first = true;
        var last_seq;

        //handle our streaming
        //returns true if the underlying res.write suceeds, false otherwise
        var stream = function stream( doc_info ) {
            
            last_seq = doc_info.seq;
            var json_string = JSON.stringify( doc_info );
            
            if ( is_first ) {
                is_first = false;
                res.write( '{"results":[\n' );
                return res.write( json_string + ",\n" );            
            }
            else {
                return res.write( json_string + ',\n' );
            }
        };
             
             
        var end_stream = function end_stream() {
            res.end( '],\n "last_seq":' + last_seq + '}\n' );
            
        };
        
        var walk_complete = false;
        var doc_info_buffer = [];
        //returns true if the underlining res.write succeeds
        var emit_from_change = function emit_from_change( doc_info ) {
          
            if ( ! walk_complete ) {
             
                doc_info_buffer.push( doc_info );
                return true;
            }
            else {                
                return stream( doc_info );   
            }
        };
        
        
        var on_db_retrieved = function on_db_retrieved( err, db ) {
        
            if ( err ) { return error_handler( err ); }
                        
            //emit doc_info every time there's a change
            var on_change = function on_change( seq, doc ) {
                
                var doc_info = { seq : seq, id : doc._id, changes : [ { rev : doc._rev } ] };
                
                if ( include_docs ) {
                    doc_info.doc = doc;
                }
                                                            
                var emit_successful = emit_from_change( doc_info );
            
                //if an error on the res stream occurs, such as writing to a disconnected socket disconnecting
                //remove the on_change listener so it doesn't get called again  
                if ( ! emit_succesful ) {
                    db.removeListener( on_change );
                    end_stream();
                }
                else if ( longpoll ) {
                    
                    db.removeListener( on_change );
                    end_stream();
                }                                
            };
            db.on( 'change', on_change );
                
            var emit_from_walk = function emit_from_walk( doc_info ) {
                
                stream( doc_info );   
            }
        
            //walk the doc_info by seq
            var on_walk_complete = function on_walk_complete( err ) {
                
                if ( err  ) {
                    //remove the listener before we error out!
                    db.removeListener( 'change', on_change );
                    return error_handler( err )
                }
                
                //go through the changes in the buffer, send out anything that's greater than
                //last_seq, ensureing the changes are up to date
                for ( var i = 0; i < doc_info_buffer.length; i++ ) {
                    var doc_info = doc_info_buffer[i];
                    if ( doc_info.seq > last_seq ) {
                        
                        stream ( doc_info );
                    }
                }
        
                walk_complete = true;

                //TODO need to detect disconnection than then remove handlers...
                if ( continous || longpoll ) {
                    //walk is now complete, from now on emit will send out the latest changes
                    if ( heartbeat ) {
                        var interval_id;
                        var on_heartbeat = function() {
                            var write_status = res.write( '\n' );
                            if ( ! write_status ) {
                                //TODO add drain support in the future
                                clearInterval( interval_id );
                                db.removeListener( 'change', on_change );
                                end_stream();
                            }
                        };
                        interval_id = setInterval( on_heartbeat, heartbeat );
                    }
  
                }
                else {
                    
                    db.removeListener( 'change', on_change );
                    end_stream();
                    
                }
            };            
            //TODO add support for begining seq and limit
            db.walk_doc_info_by_seq( emit_from_walk, on_walk_complete, undefined, undefined, include_docs );
        };
        dbm.retrieve_db( db_name, on_db_retrieved );
    });
};