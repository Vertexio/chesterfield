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

    //TODO implement this
    app.get('/:db/_changes', function(req, res) {

        var name = req.params.db;
        
        if (req.query.feed !== 'continuous') {
            res.json({ last_seq: 0, results: [] });
            return;
        }

        var opts = {};
        var hbid;
        var sub = Memcouch.dbSubscribe(req.params.db, opts, req, function(json) {
            res.write(JSON.stringify(json) + '\n');
        }, function() {
            if (hbid) { clearInterval(hbid); }
            res.end();
        });

        if (sub.error) {
            res.json(sub, status[sub.error]);
            return;
        }

        // Heartbeat to keep the connection alive
        hbid = setInterval(function() {
            res.write('\n');
        }, opts.heartbeat || 60000);

        res.useChunkedEncodingByDefault = true;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        req.connection.on('end', function() {
            Memcouch.dbUnsubscribe(req.params.db, sub);
        });
    });

    //TODO implement compaction
    app.post('/:db/_compact', function(req, res) {
        res.json({ ok: true });
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
};