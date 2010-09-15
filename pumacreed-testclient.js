var DNode = require('dnode');
var sys = require('sys');

var voidfn = function () {};
var server = null;

var sleep_map = function(k, v, emit) {
    var startTime = new Date();
    var endTime = null;
    do {
        endTime = new Date();
    } while ((endTime - startTime) < (v * 2000));
    emit(k,k);
}

var job = { mapfn : sleep_map.toString(), reducefn: null};

var job_results = function (map, cb) {
    for (var key in map) {
        console.log(key, ": ", map[key]);
    }
    server.end();
}

var connection = DNode({ job_results: job_results}).connect(6060, 
    function (remote, conn) {
        console.log("Connected");
        remote.create_job("spinjob", [[1,2],[2,1],[3,1]], job, true, voidfn);
        server = conn;
});

