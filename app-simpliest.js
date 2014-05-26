var config = {
    "host": "localhost",
    "port": "8098"
};

var uuid = require('node-uuid');
var Promise = require("bluebird");
var db = require('riak-js').getClient( config );
var dbSave = Promise.promisify( db.save.bind(db) );
var dbSaveBucket = Promise.promisify( db.saveBucket, db );
var dbRemove = Promise.promisify( db.remove, db );
var searchFind = Promise.promisify( db.search.find, db.search );

var obj = require("./data/obj.json")

var prefix1 = "8598900546019-";
var bucket = "test-index-consistency";

function runTest( id ){
    console.log( "run test for", id )

    obj.originalID =  id;
    obj.id =   prefix1 + id;

    console.log( "save to bucket")
    return dbSave( bucket, obj.id, obj )
    .then(function(result){
        console.log( "remove from bucket")
        return dbRemove(bucket, obj.id );
    })
    .then(function(){
        return validateConsistency( id, "After Removed" )
    })
}

function runTests( ids ){
    var id = ids.shift();
    if ( id )
        return runTest( id)
            .then(function(){
                return runTests(ids);
            })
}

function main(){

    var ids = []
    for ( var i =0; i< 100; i++)
        ids.push( uuid.v4() );

    return Promise.all([ enableSearch(bucket)])
        .then (function(){
            return runTests(ids);
        })

.catch( function(err){
      console.error(err.stack);
    })
}


function validateConsistency ( originalId, where ){
    return searchFind( bucket, "originalID:" + originalId )
        .then(function( result ){
            if( result[0].numFound != result[0].docs.length )
                throw new Error("Index broken at " +  where);
        })
}




function enableSearch (bucket){
    return dbSaveBucket(bucket, {search: true, last_write_wins:false, allow_mult: true } );
}



if (require.main === module) {
    main();
}