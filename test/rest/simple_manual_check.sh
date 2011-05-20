curl -X DELETE -H "Content-Type: application/json" http://localhost:5985/foo 
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo 
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo/bar -d '{ "foo" : "bar" }' 
curl -X PUT -H "Content-Type: application/json" http://localhost:5985/foo/yeah -d '{ "booya" : "grandma" }' 
