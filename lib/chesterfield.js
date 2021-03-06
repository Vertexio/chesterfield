/*
   Copyright 2011 Geoff Flarity
   Copyright 2011 pcapr
   
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applica   ble law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/



var Express = require('express');
//var Server = require('./server_api');
var database_api = require('./database_api');
var document_api = require('./document_api');


var DBManager = require('./db_manager').DBManager;


//this funcion returns a function that extends express'es req, res, and next
//TODO is this the standard way to do this with express? 
function helpers() {
    return function(req, res, next) {
        // Output JSON objects
        res.json = function(obj, status, headers) {
            res.useChunkedEncodingByDefault = false;
            headers = headers || {};
            headers['Content-Type'] = 'application/json';
            res.writeHead(status || 200, headers);
            res.end(JSON.stringify(obj) + '\n');
        };
        
        //TODO add non application/json stuff helper

        res.niy = function(obj) {
            res.json({ error: 'niy', reason: 'not implemented yet' }, 501);
        };

        next();
    };
}

//once the dbm is ready
var dbm;
var on_dbm_ready = function on_dbm_ready( err ) {
    
    if ( err ) {
        console.log(err);            
        process.exit(1);
    }
    
    var app = Express.createServer(Express.logger(), helpers());
    app.use(Express.bodyParser());
    
    app.use('/_utils/', Express.static('./www/'));
    
    app.get('/_config/query_servers/', function(req, res) {
		// TODO: this is mock
		 res.json({"javascript":"couchdb_1.0.2/bin/couchjs couchdb_1.0.2/share/couchdb/server/main.js"});
	});
	
	app.get('/_config/native_query_servers/', function(req, res) {
		res.json({});
	});

    app.get('/', function(req, res) {
        res.json({ couchdb: 'Welcome', memcouchdb: 'Yo!', version: '0.0.1' });
    });


    database_api.mount(app, dbm);
    document_api.mount(app, dbm);

    //TODO is there a better home for these?
    //TODO implement stats
    app.get('/_stats', function(req, res) {
        res.niy();
    });

    //TODO implement active tasks
    app.get('/_active_tasks', function(req, res) {
        res.niy();
    });

    //TODO implement restart
    app.post('/_restart', function(req, res) {
        res.json({ ok: true });
    });
    
        //TODO implement me
    app.get('/_config', function(req, res) {
        res.niy();
    });

    //TODO expose UUID functionality
    app.get('/_uuids', function(req, res) {
        var i, count = req.query.count || 1, json = { uuids: [] };
        for (i=0; i<count; ++i) {            
            json.uuids.push(uuid_generator());
        }
        res.json(json, 200, { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' });
    });


    app.listen(5985);
    console.log('listening on localhost:5985');
  
};
dbm = new DBManager('/var/tmp/chesterfield', on_dbm_ready );
    