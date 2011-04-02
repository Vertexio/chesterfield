/*
   Copyright 2011 Geoffrey Flarity

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

var fs = require('fs');
var ctype = require('./ctype');

var DEFAULT_OPTIONS = { 

};

function DocumentLog( file_path, options ) {

    if( ! file_path ) {
	throw "file_path is a required paramter";
    }

    var options = options || DEFAULT_OPTIONS;

    this.fd = fs.openSync(file_path, 'a+');

    var stat = fs.statSync( file_path );

    this.position = stat.size;
    this.sequence_number = 0;
    if ( this.position > 0 && this.position < 16) {
	console.log(file_path + ' appears to be corrupted');
    } 
    else if ( this.position > 0 ) {


	//read backwards to get the last sequence number
	var buffer = new Buffer(8);
	fs.readSync( this.fd, buffer, 0, buffer.length, this.position - 8 );

	var payload_length = ctype.rdouble( buffer, 'little', 0 );
	fs.readSync( this.fd, buffer, 0, buffer.length, this.position - 8 - payload_length );

	//first 8 bytes of payload should be sequence number
	this.sequence_number = ctype.rdouble( buffer, 'little', 0 );
    }
    

    //integer check
    if ( Math.floor(this.sequence_number) != this.sequence_number ) {
	console.log( "sequence number is not an integer: " + this.sequence_number );
	process.exit(1);
    }


}
exports.DocumentLog = DocumentLog;

/* 
   document - a string representing a JSON document
   on_append_complete - callback(sequence_number, reference, length )
   
   Each entry looks like :
   | L | SN | Doc | L |
   
   L - Length (8 byte double float aka javascript Number )
   SN - Sequence Number ( 8 byte double float aka javascript Number )  
*/

DocumentLog.prototype.append = function append( document, on_append_complete ) {

    if ( typeof(document) != 'string' ) {
	throw "document must be a string";
    }
    
    if ( typeof(on_append_complete) != 'function' ) {
	throw "on_append_complete must be a function";
    }
    var payload_length = document.length  + 8; //8 for the sequence number
    var new_sequence_number = this.sequence_number + 1;

    var buffer = Buffer( payload_length + 16 );
    ctype.wdouble( payload_length, 'little', buffer, 0 );
    ctype.wdouble( new_sequence_number, 'little', buffer, 8 );
    ctype.wdouble( payload_length, 'little', buffer, buffer.length - 8 );
    
    buffer.write( document, 16 );

    var self = this;

    var on_write_complete = function on_write_complete(err, written, buffer ) {
	
	//TODO need a better way of handling errors, clearly writting can't continue until maintenance occurs
	//     but we don't need to exit, could go into maintenance required mode or something instead
	if (err ) {
	    console.log(err)
	    console.log("wrote " + written  + ' bytes, possible corruption of log as occured');
	    process.exit(1)
	}

	if ( written < buffer.length ) {
	    console.log('only wrote ' + written + ' bytes, corruption has occured' );
	    process.exit(1);
	}

	//all is well
	self.position += buffer.length;
	var reference = self.position - buffer.length + 16; //end of last write + 16 to skip the length + sequence number
	on_append_complete( null, new_sequence_number, 
			    reference,
			    document.length );
    }

    fs.write(self.fd, buffer, 0, buffer.length, null, on_write_complete );
};

DocumentLog.prototype.retrieve = function retrieve( reference, length, on_retrieved ) {

    if ( reference + length > this.position ) {

	throw "Attempt to read past end of log";
    }

    var buffer = new Buffer(length);
    var on_read = function on_read( err, bytesRead ) {

	if (err) {
	    return on_retrieved( err );
	}
	
	if ( bytesRead < length ) {

	    err = "failed to read entire length";
	    return on_retrieved( err );
	}

	//otherwise all is well
	var payload = buffer.toString();
	return on_retrieved( err, payload );
	
    }


    fs.read(this.fd, buffer, 0, length, reference, on_read )
    

}