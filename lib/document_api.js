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

var fs = require('fs');

var QueryString = require('querystring');
var generate_uuid = require('node-uuid');

this.mount = function(app, dbm) {
    var status = {
        'illegal_database_name': 400,
        'bad_request': 400,
        'not_found': 404,
        'conflict' : 409,
        'file_exists': 412,
        'internal_server_error': 500
    };
    
    var _retrieve_db_helper = function _retrieve_db_helper( name, on_retrieved_successfully ) {
        
        var on_retrieved = function on_retrieved( err, db ) {
             
            if ( err ) {
                
                json.error = 'not_found';
                return res.json(json, status[json.error] );
            }
            else {
                on_retrieved_successfully( db );
            }
            
        };
        dbm.retrieve_db( name, on_retrieved);
        
    };
    
    var _parseViewOptions = function(params) {
        var n, opts = {};

        var parsers = {
            limit: function(v) {
                return parseInt(v, 10);
            },
            key: function(v) {
                return JSON.parse(v);
            },
            startkey: function(v) {
                return JSON.parse(v);
            },
            startkey_docid: function(v) {
                return v;
            },
            endkey: function(v) {
                return JSON.parse(v);
            },
            endkey_docid: function(v) {
                return v;
            },
            descending: function(v) {
                return v === 'true' ? true : v === false ? false : undefined;
            }
        };

        for (n in params) {
            if (params.hasOwnProperty(n) && parsers.hasOwnProperty(n)) {
                opts[n] = parsers[n](params[n]);
            }
        }

        return opts;
    };

    var _doView = function(req, res, cb) {
        var opts = _parseViewOptions(req.query);
        var output = { rows: [] };
        var json = cb(opts, function(row) { output.rows.push(row); });
        if (json.error) {
            res.json(json, status[json.error]);
        } else {
            output.total_rows = json.total_rows;
            output.offset = json.offset;
            res.json(output, 200);
        }
    };

    //all docs
    app.get('/:db/_all_docs', function(req, res) {

        var db_name = req.params.db;
        
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
             
                var last_item = doc_history[doc_history.length-1];
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
                    
                    var json_string = JSON.stringify( doc );
                    if ( is_first ) {
                        res.write('foo');    
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

    
    //TODO temp view
    app.post('/:db/_temp_view', function(req, res) {
        res.niy();
    });

    //TODO views
    //TODO offsets, startkeys endkeys etc
    app.get('/:db/_design/:design/_view/:view', function(req, res) {
        
        var db_name = req.params.db;
        var view_name = req.params.view;
        
        var rows = [];
        var emit = function emit( key, value, id ) { rows.push({ 'key' : key, 'value' : value, 'id' : id } ); };
        var on_complete = function on_complete( err ) {
            
            if ( err ) {
             
                return res.json( { error : 'internal_server_error' },  status[json.error] );
            }

            res.json( {  'total_rows' : rows.length, 'rows' : rows } );
        };        
        var on_db_retrieved_successfully = function on_db_retrieved_successfully( db ) {
            
            var on_view_retrieved = function on_view_retrieved( err, view ) {                
                        
                if ( err ) {
                        
                    res.json( { error : 'not_found' }, status['not_found'] );
                }
            
                view.walk( emit, on_complete );         
            };
            db.retrieve_view( view_name, on_view_retrieved )
            
        };
   
    });
    
    //TODO bulk docs
    app.post('/:db/_bulk_docs', function(req, res) {
        res.niy();
    });

    var _doc_id = function(params) {
        // need to handle _design views where doc id is _design and child id is the name of the view 
        var child = params.child ? "/" + params.child : "";
        return QueryString.unescape(params.doc_id + child);
    }


    //TODO add support for HEAD requests
  
    // get a document
    app.get('/:db/:doc_id', function(req, res) {
        var db_name = req.params.db;
        var doc_id = req.params.doc_id;
        var rev = req.params.rev;
        
        var on_db_retrieved = function on_db_retrieved( err, db ) {
            
            var on_doc_retrieved = function on_doc_retrieved( err, doc ) {

                if ( err ) { 
                    
                    return res.json( { error : 'not_found', reason : err }, status['not_found'] ); 
                }
                else {
                    
                    res.json( doc );
                }
        
            };
            db.retrieve_doc( doc_id, rev, on_doc_retrieved );
        };
        dbm.retrieve_db( db_name, on_db_retrieved );
    
    });
    
      // get a document attachment
    app.get('/:db/:doc_id/:attachment_file_name', function(req, res) {
        var db_name = req.params.db;
        var doc_id = req.params.doc_id;
        var attachment_file_name = req.params.attachment_file_name;
        
        var on_db_retrieved = function on_db_retrieved( err, db ) {
            
            var on_doc_retrieved = function on_doc_retrieved( err, doc ) {

                if ( err ) { 
                    
                    return res.json( { error : 'not_found', reason : err }, status['not_found'] ); 
                }

                //TODO get the attachment_stream, pipe it into req
                var on_stream_ready = function( err, content_type, attachment_stream ) {
                    
                    if ( err ) { return res.json( { error : 'not_found', reason : err }, status['not_found'] ); }
                    
                    var headers =  res.headers || {};
                    headers['content-type'] = content_type;
                    res.writeHead(status || 200, headers);
                    attachment_stream.pipe( res );
                };
                db.retrieve_attachment_stream( doc_id, attachment_file_name, on_stream_ready );
                //TODO does retrieve_attachment_stream need to pause the stream first?
            };
            db.retrieve_doc( doc_id, undefined, on_doc_retrieved );
        };
        dbm.retrieve_db( db_name, on_db_retrieved );
    
    });
    
      //create or update a document
    app.put('/:db/:doc_id/:attachment_file_name', function(req, res) {

        var name = req.params.db;
        var doc_id = req.params.doc_id;
        var attachment_file_name = req.params.attachment_file_name;
        var rev = req.query.rev;
        var content_type = req.headers['content-type'];
        
        var error_handler = function error_handler( err ) {
            
            //TODO everything is an internal server error for now but should
            //be done properly eventually
            var json = { error : 'internal_server_error', reason: err };
            return res.json(json, status[json.error] );            
        }
        
        //since there is an attachment then we need set on data callbacks before any async
        //stuff otherwise the data will be read and lost
        var uuid = generate_uuid();
        var tmp_file_name = '/var/tmp/' + uuid;
        var write_stream = fs.createWriteStream( tmp_file_name );
        
        var file_size = 0;
        req.on('data', function(chunk) {
            file_size += chunk.length;
            write_stream.write( chunk );
        });
        
        
        var on_file_closed = function on_file_closed() {
            
            var on_db_retrieved = function on_db_retrieved( err, db ) { 
            
                if ( err ) { return error_handler( err ); }
                    
                var on_attached = function on_attached( err, new_doc ) {
                     
                    if ( err ) { return error_handler( err ) };
                    
                    var json = { ok : true, id : new_doc._id, rev : new_doc._rev };
                    return res.json(json, status[json.error] || 201 );  
                };
                db.attach( doc_id, rev, attachment_file_name, content_type, file_size, tmp_file_name, on_attached );    
            };
            dbm.retrieve_db( name, on_db_retrieved )
        
        };        
        req.on('end', function( chunk ) { 
                if ( chunk ) {
                    file_size += chunk.length;
                }
                write_stream.end( chunk );
                write_stream.on( 'close', on_file_closed );
                write_stream.destroySoon();
        });
              
    });

    //create or update a document
    app.put('/:db/:doc_id', function(req, res) {

        var name = req.params.db;
        var doc_id = req.params.doc_id;
        var attachment = req.params.attachment;
        var rev = req.params.rev;

        var error_handler = function error_handler( err ) {
            
            //TODO everything is an internal server error for now but should
            //be done properly eventually
            var json = { error : 'internal_server_error', reason: err };
            return res.json(json, status[json.error] );            
        }
            
        var on_db_retrieved = function on_db_retrieved( err, db ) { 

            //TODO need error handling abstraction
            
            if ( err ) { return error_handler( err ); }

            var doc = req.body;
            doc._id = doc_id;

            var on_doc_added_or_updated = function on_doc_added_or_updated( err, doc ) {

                var json;
                if ( err ) {
                    
                    json = { error : 'bad_request', reason : err  };                
                    return res.json( json, status[json.error] );
                }

                json = { ok : true, id : doc._id, rev : doc._rev };
                
                var headers = {};
                if (!json.error) {
                    //TODO what's this all about?
                    headers['Location'] = 'http://' + req.headers.host + '/' + req.params.db + '/' + json.id;
                }
                return res.json(json, status[json.error] || 201, headers);
            };
            
            db.add_or_update_doc( doc, on_doc_added_or_updated );
        };
        dbm.retrieve_db( name, on_db_retrieved )  
    }); 
    
    //upload an attachment
    app.put('/:db/:doc_id/:attachment', function(req, res) {
    
    });

    //create a document
    app.post('/:db', function(req, res) {
        var name = req.params.db;
        
        var doc = req.body;
        
        if ( ! doc._id ) {
            doc._id = generate_uuid();    
        }
                                
        var on_db_retrieved = function on_db_retrieved( err, db ) { 

            //TODO need error handling abstraction
            
            if ( err ) {
                var json = {};
                json.error = 'bad_request';                
                json.reason = err;
                return res.json( json, status[json.error] );
            }

            var on_doc_added_or_updated = function on_doc_added_or_updated( err, doc ) {
                var json;
                if ( err ) {
                    
                    json = { error : 'bad_request', reason : err  };                
                    return res.json( json, status[json.error] );
                }

                json = { ok : true, id : doc._id, rev : doc._rev };
                
                var headers = {};
                if (!json.error) {
                    //TODO what's this all about?
                    headers['Location'] = 'http://' + req.headers.host + '/' + req.params.db + '/' + json.id;
                }
                return res.json(json, status[json.error] || 201, headers);
                
                
            };
            
            db.add_or_update_doc( doc, on_doc_added_or_updated );
                
        };
        dbm.retrieve_db( name, on_db_retrieved )
      
    }); 

    // delete a document
    app.del('/:db/:doc_id/:child?', function(req, res) {
        
        var db_name = req.params.db;
        var doc_id = req.params.doc_id
                
        //TODO support header rev
        var rev = req.query.rev;
        
        if ( ! rev ) {            
            var json = { error: 'conflict', reason: 'Document update conflict.' };            
            return res.json( json, status[json.error] );
        };
        
        var on_db_retrieved = function on_db_retrieved( err, db ) { 

            if ( err ) {
                
                var json = { error : 'bad_request', reason : err };
                return res.json( json, status[json.error] );
            }

            var on_doc_deleted = function on_doc_deleted( err, new_rev ) {
                
                json = { id : doc_id, rev : new_rev, ok : true };
                
                return res.json( json, 200 );                
            };
            db.delete_doc( doc_id, rev, on_doc_deleted );

        };
        dbm.retrieve_db( name, on_db_retrieved )
        
    });
};
