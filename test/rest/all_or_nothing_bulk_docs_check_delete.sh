echo 'first sweep'
curl -X DELETE -H "Content-Type: application/json" http://localhost:5985/foo 
echo '=> {"ok":true}'
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo 
echo '=> {"ok":true}'
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo/1 -d '{}' 
echo '=> {"ok":true,"id":"1","rev":"1-6290fecaeba293a275869e78f3725341"}'
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo/1 -d '{ "_rev" : "1-6290fecaeba293a275869e78f3725341", "foo" : "bar" }'
echo '=> {"ok":true,"id":"1","rev":"2-d70c367bc9a040f040defacfa39b87aa"}'
curl -X POST -H "Content-Type: application/json" http://localhost:5985/foo/_bulk_docs -d '{ "all_or_nothing" : true, "docs" : [ { "_id" : "1", "_rev" : "1-6290fecaeba293a275869e78f3725341", "test" : true }] }'
echo '=> [{"id":"1","rev":"2-0ee60f1c75481de733d4315db3affcab"}]'
curl -X GET -H "Content-Type: application/json" http://localhost:5985/foo/1
echo '=> {"_rev":"2-d70c367bc9a040f040defacfa39b87aa","foo":"bar","_id":"1"}'
curl -X DELETE -H "Content-Type: application/json" http://localhost:5985/foo/1?rev=2-d70c367bc9a040f040defacfa39b87aa
echo '=> {"id":"1","rev":"3-10265ab17442880f2a87f484f703cad4","ok":true}'
curl -X GET -H "Content-Type: application/json" http://localhost:5985/foo/1
echo '=> {"_id":"1","_rev":"2-0ee60f1c75481de733d4315db3affcab","test":true}'

