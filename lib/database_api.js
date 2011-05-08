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

this.mount = function(app) {
    var status = {
        'illegal_database_name': 400,
        'bad_request': 400,
        'not_found': 404,
        'file_exists': 412
    };

    //TODO database creation
    app.put('/:db', function(req, res) {
        var json = Memcouch.dbCreate(req.params.db);
        var headers = {};
        headers['Location'] = 'http://' + req.headers.host + '/' + req.params.db;
        res.json(json, status[json.error] || 201, headers);
    });

    //TODO db info request
    app.get('/:db', function(req, res) {
        var json = Memcouch.dbInfo(req.params.db);
        res.json(json, status[json.error] || 200);
    });

    //TODO db delete
    app.del('/:db', function(req, res) {
        if (req.query.rev) {
            res.json({ error: 'bad_request', reason: 'You tried to DELETE a database with a ?=rev parameter. Did you mean to DELETE a document instead?' }, 400);
        } else {
            var json = Memcouch.dbDelete(req.params.db);
            res.json(json, status[json.error] || 200);
        }
    });

    //TODO changes feed
    // This is primarily a feed=continuous implementation. The big difference
    // between couch and this is, you don't get at the various changes if you are
    // not watching the feed. In that sense it's more like redis' pubsub.
    //
    // TODO:
    // - add support for filtered changes
    // - support include_docs in the req.query
    // - when the filter changes or the design doc gets updated, same thing
    // - I'm sure there's a bunch of other corner cases
    app.get('/:db/_changes', function(req, res) {
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

    //TODO compaction
    app.post('/:db/_compact', function(req, res) {
        res.json({ ok: true });
    });

    //TODO ensure full commit
    app.post('/:db/_ensure_full_commit', function(req, res) {
        var json = Memcouch.dbGet(req.params.db, function(db) {
            return { ok: true, instance_start_time: Memcouch._start_time.toString() }
        });
        res.json(json);
    });
};