//TODO make this indentical to couchdb ordering: 
//http://wiki.apache.org/couchdb/View_collation?action=show&redirect=ViewCollation 

var cmp = function cmp( a, b ) {

    if ( typeof a == 'object' )  { 

	throw "complex sorting has not been implemented yet";

    }
    else {

	return a == b ? 0 : a < b ? -1 : 1;

    }

};

exports.cmp = cmp;
