//TODO Eventually use node-tap to write proper tests

var DocumentLog = require('../lib/document_log').DocumentLog;
var dl = new DocumentLog( 'data/test.log');

var sequence_number_before = dl.sequence_number;

var data = { foo : 'bar'};

var on_append_complete = function on_append_complete( err, sequence_number, reference, length ) {
    
    if ( err ) {
	console.log(err);
	process.exit(1);
    }
    
    var on_retrieved = function on_retrieved( err, payload ) {
	console.log( JSON.stringify(data) == payload );
	console.log( sequence_number_before + 1 == sequence_number );
    }

    dl.retrieve( reference, length, on_retrieved );
}

dl.append( JSON.stringify(data), on_append_complete );
