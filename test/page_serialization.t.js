var fs = require('fs');

var empty_page = [];
for (var i = 0; i < 4096; i++ ) {
    empty_page.push(0);
}
var buffer = new Buffer(empty_page);

var data = { keys : [0, 1, 2, 3], child_pages : [0,1,2,3] };
var serialized_data = JSON.stringify(data);
buffer.write( serialized_data, 0, 'binary' );

var fd = fs.openSync('./data/page_serialize.data', 'r+');
fs.writeSync(fd, buffer, 0, 4096);
var read_buffer = new Buffer(4096);
var read_data = fs.readSync(fd, read_buffer, 0, 4096, 0 );
console.log(read_buffer.toString());


