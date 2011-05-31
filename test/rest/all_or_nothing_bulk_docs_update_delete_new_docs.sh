curl -X DELETE -H "Content-Type: application/json" http://localhost:5985/foo 

curl -X POST -H "Content-Type: application/json" http://geoff:timesgo@localhost:5985/foo/_bulk_docs -d '{ "all_or_nothing" : true, "docs" : [ { "_rev" : "6-55fa92c1a80a923f12ca22e79eefded1",  "_id" : "1"}] }'
curl -X POST -H "Content-Type: application/json" http://geoff:timesgo@localhost:5985/foo/_bulk_docs -d '{ "all_or_nothing" : true, "docs" : [ { "_rev" : "4-55fa92c1a80a923f12ca22e79eefded1",  "_id" : "2"}] }'
