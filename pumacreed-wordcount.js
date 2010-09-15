var DNode = require('dnode');
var sys = require('sys');
var fs = require('fs');

var voidfn = function () {};
var server = null;
var starttimemr, starttimestraight;

var wordcount_map = function(k, v, emit) {
    var tokens = v.split(" ");
    for (var t in tokens) {
        if (tokens[t].length > 0)
            emit(tokens[t], 1);
    }
}
var wordcount_reduce = function(k, v, emit) {
    var sum = 0;
    for (var i in v) sum += v[i];
    if (sum > 100)
        emit(k,sum);
}

var jobspec = { 
    name: "wordcountjob",
    mapfn : wordcount_map.toString(), 
    reducefn: wordcount_reduce.toString()
    };

var job_results = function (map, cb) {
    final = (new Date()) - starttimemr;
    console.log(final);
    for (var key in map) {
        console.log(key, ": ", map[key]);
    }
    server.end();
}

console.log("Loading")
var file = fs.readFileSync(process.argv[2]);
var lines = file.toString().split("\n");
for (var i in lines) {
    lines[i] = [i, lines[i]];
}
var data = lines;

starttimestraight = new Date();
store = {};
for (var i in data) {
    var tokens = lines[i][1].split(" ");
    for (var t in tokens) {
        if (tokens[t].length > 0)
        {
            if (!(tokens[t] in store)) store[tokens[t]] = 0;
            store[tokens[t]] += 1
        }
    }
}
for (var key in store) {
    if (store[key] < 101) {
        delete store[key];
    }
}

final = (new Date()) - starttimestraight;
console.log(final);

var connection = DNode({ job_results: job_results}).connect(6060, 
    function (remote, conn) {
        console.log("Connected");
        remote.create_job(jobspec.name, data, jobspec, true, function(){ starttimemr = new Date(); console.log("Working...")});
        server = conn;
});

