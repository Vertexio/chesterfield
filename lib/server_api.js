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

this.mount = function(app) {
    app.get('/_all_dbs', function(req, res) {
        //TODO get all databases
        res.json(Memcouch.allDbs());
    });

    //TODO implement me
    app.get('/_config', function(req, res) {
        res.niy();
    });

    //TODO expose UUID functionality
    app.get('/_uuids', function(req, res) {
        var i, count = req.query.count || 1, json = { uuids: [] };
        for (i=0; i<count; ++i) {
            json.uuids.push(Memcouch.uuid());
        }
        res.json(json, 200, { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' });
    });

    //TODO what's session all about?
    app.get('/_session', function(req, res) {
        res.json({
            ok: true,
            userCtx: { name: null, roles: [ '_admin' ] },
            info: {
                authentication_db: '_users',
                authentication_handlers: [ 'cookie', 'default' ]
            }
        });
    });

    //TODO implement replication
    app.post('/_replicate', function(req, res) {
        res.niy();
    });

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
};