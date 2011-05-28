var crypto = require('crypto');

function sort_object(o) {
    var sorted = {},
    key, a = [];
    
    for (key in o) {
        if (o.hasOwnProperty(key)) {
                a.push(key);
        }
    }
    
    a.sort();
    
    for (key = 0; key < a.length; key++) {
        //TODO recursively sort the children
        sorted[a[key]] = o[a[key]];
    }
    return sorted;
}

exports.doc_md5sum = function doc_md5sum( doc ) {
    
    sorted_doc = sort_object( doc );
    var doc_string = JSON.stringify( sorted_doc );
    md5_digester = crypto.createHash('md5');
    md5_digester.update(doc_string);
    return md5_digester.digest('hex');    
};