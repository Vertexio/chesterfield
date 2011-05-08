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

var Store = require('./store');
var QueryString = require('querystring');

this.mount = function(app) {
    var status = {
        'illegal_database_name': 400,
        'bad_request': 400,
        'not_found': 404,
        'file_exists': 412
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

    //TODO all docs
    // TODO: Need to stream this over instead of accumulating all the docs
    // in memory first, which means we do JSON.stringify on each row, but
    // the rest of the surrounding JSON is vanilla res.write
    app.get('/:db/_all_docs', function(req, res) {
        _doView(req, res, function(opts, cb) {
            return Memcouch.dbAllDocs(req.params.db, opts, cb);
        });
    });

    //TODO all docs
    app.post('/:db/_all_docs', function(req, res) {
        _doView(req, res, function(_, cb) {
            return Memcouch.dbGetDocs(req.params.db, req.json, cb);
        });
    });

    //TODO temp view
    app.post('/:db/_temp_view', function(req, res) {
        _doView(req, res, function(opts, cb) {
            return Memcouch.dbTempView(req.params.db, req.json, opts, cb);
        });
    });

    //TODO view
    app.get('/:db/_design/:design/_view/:view', function(req, res) {
        _doView(req, res, function(opts, cb) {
            return Memcouch.dbView(req.params.db, req.params.design, req.params.view, opts, cb);
        });
    });
    
    //TODO bulk docs
    app.post('/:db/_bulk_docs', function(req, res) {
        var json = Memcouch.dbBulkDocs(req.params.db, req.json);
        res.json(json, status[json.error] || 201);
    });

    var _doc_id = function(params) {
        // need to handle _design views where doc id is _design and child id is the name of the view 
        var child = params.child ? "/" + params.child : "";
        return QueryString.unescape(params.doc_id + child);
    }

    //TODO read doc, child should be revision id?
    // read a document
    app.get('/:db/:doc_id/:child?', function(req, res) {
        var json = Memcouch.dbGetDoc(req.params.db, _doc_id(req.params), req.query);

        var headers = json.error ? {} : { 'ETag': json._rev };
        res.json(json, status[json.error] || 200, headers);
    });

    //TODO update document
    // update a document
    app.put('/:db/:doc_id/:child?', function(req, res) {
        var json = Memcouch.dbPutDoc(req.params.db, _doc_id(req.params), req.json);
        if (req.query.batch === 'ok') {
            res.json({ ok: true }, 202);
            return;
        }
        
        var headers = {};
        if (!json.error) {
            headers['Location'] = 'http://' + req.headers.host + '/' + req.params.db + '/' + json.id;
        }
        res.json(json, status[json.error] || 201, headers);
    });

    //TODO create a document
    app.post('/:db', function(req, res) {
        var json = Memcouch.dbPutDoc(req.params.db, undefined, req.json);
        if (req.query.batch === 'ok') {
            res.json({ ok: true }, 202);
            return;
        }
        
        var headers = {};
        if (!json.error) {
            headers['Location'] = 'http://' + req.headers.host + '/' + req.params.db + '/' + json.id;
        }
        res.json(json, status[json.error] || 201, headers);
    });

    //TODO delete a document
    app.del('/:db/:doc_id/:child?', function(req, res) {
        var json = Memcouch.dbDeleteDoc(req.params.db, _doc_id(req.params), req.query.rev);
        res.json(json, status[json.error] || 200);
    });
};