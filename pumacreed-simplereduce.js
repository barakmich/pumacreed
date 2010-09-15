var DNode = require('dnode');
var sys = require('sys');

var voidfn = function () {};
var server = null;

var wordcount_map = function(k, v, emit) {
    emit(k,v);
}
var wordcount_reduce = function(k, v, emit) {
    sum = 0;
    for (var i in v) sum += v[i];
    emit(k,sum);
}

var job = { mapfn : wordcount_map.toString(), reducefn: wordcount_reduce.toString()};

var job_results = function (map, cb) {
    for (var key in map) {
        console.log(key, ": ", map[key]);
    }
    server.end();
}

var connection = DNode({ job_results: job_results}).connect(6060, 
    function (remote, conn) {
        console.log("Connected");
        remote.create_job("simplereducejob", [['foo',2],['bar',1],['foo',1]], job, true, voidfn);
        server = conn;
});

