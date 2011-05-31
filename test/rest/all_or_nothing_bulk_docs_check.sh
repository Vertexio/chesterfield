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
curl -X POST -H "Content-Type: application/json" http://localhost:5985/foo/_bulk_docs -d '{ "all_or_nothing" : true, "docs" : [ { "_id" : "1", "_rev" : "2-d70c367bc9a040f040defacfa39b87aa", "test" : true }] }'
echo '=> [{"id":"1","rev":"3-dd4f76807bbf0f0b15b52969b9966cdc"}]'
curl -X GET -H "Content-Type: application/json" http://localhost:5985/foo/1
echo '=> {"_id":"1","_rev":"3-dd4f76807bbf0f0b15b52969b9966cdc","test":true}'
curl -X POST -H "Content-Type: application/json" http://localhost:5985/foo/_bulk_docs -d '{ "all_or_nothing" : true, "docs" : [ { "_id" : "1", "_rev" : "3-dd4f76807bbf0f0b15b52969b9966cdc", "_deleted" : true }] }'
echo '=> [{"id":"1","rev":"4-ca98e11185ba4a5a4ee6e324eea9ebdd","_deleted":true}]'
curl -X GET -H "Content-Type: application/json" http://localhost:5985/foo/1
echo '=> {"_id":"1","_rev":"2-0ee60f1c75481de733d4315db3affcab","test":true}'

