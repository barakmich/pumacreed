var http = require('http');
var DNode = require('dnode');
var fs = require("fs");
var path = require("path");
var sys = require("sys");
var events = require("events");
var pumahttp = require('./pumacreed-http');

var job_emitter = new events.EventEmitter();

var job_list = [];
var jobs = {};
var cubs = {};
var voidfn = function () {};
var REDUCE = 2;
var counter = 0;
var MAP = 1;

function return_results(job) {
    for (var i in job.clients) {
        job.clients[i].job_results(job.results);
    }
    job_list.shift();
    delete jobs[job.name]
}

function map_result(jobname, key, value) { 
    var job = jobs[jobname]
    if (!(key in job.reduce_data)) {
        job.reduce_data[key] = [];
        job.reduce_keys.push(key);
    }
    //console.log(job.reduce_data);
    job.reduce_data[key].push(value);
}

function reduce_result(jobname, key, value) { 
    var job = jobs[jobname]
    if (!(key in job.results)) job.results[key] = [];
    job.results[key].push(value);
}

function finish_task(cub,results) {
    var f = cub.tasks.shift();
    //map the job lists
    var active_job = jobs[results.jobname];
    //console.log("RESULTS");
    //console.log(results.data.length);
    for (var i in results.data) {
        //console.log(results.data[i]);
        if (active_job.phase == MAP) {
            map_result(active_job.name, results.data[i][0], results.data[i][1]);
        }
        if (active_job.phase == REDUCE) {
            reduce_result(active_job.name, results.data[i][0], results.data[i][1]);
        }
    }

    active_job.active_count -= 1; 
    if (active_job.active_count == 0) {
        if (active_job.phase == MAP && active_job.map_data.length == 0) {
            if (active_job.code.reducefn == null) {
                active_job.results = active_job.reduce_data;
                return_results(active_job);
            }
            else {
                console.log("Begin REDUCE");
                //console.log(active_job.reduce_keys);
                //console.log(active_job.reduce_data);
                active_job.phase = REDUCE;
                job_emitter.emit('schedule');
            }
        }
        else if (active_job.phase == REDUCE && active_job.reduce_keys.length == 0) {
            return_results(active_job);
        }
        else 
            job_emitter.emit('schedule');
    }
    else {
        job_emitter.emit('schedule');
    }
}

job_emitter.on('schedule', function () {
    //What things need doing?
    var got_one = false;
    if (job_list.length == 0) return;
    var job = jobs[job_list[0]];
    //Who's free?
    for (var hostkey in cubs) {
        var cub = cubs[hostkey];
        if (cub.tasks.length == 0) {
            //console.log("Scheduling", hostkey);
            //console.log(job);
            if (job.map_data.length > 0) {
                if (job.code.mapfn != null) {
                    if (!(job.name in cub.active_jobs))
                        send_job(cub, job);
                    var kv = job.map_data.shift();
                    cub.tasks.push([job.name, kv]);
                    //console.log(cub.tasks)
                    job.active_count += 1;
                    cub.rpc.do_map(job.name, [ kv ], finish_task.bind(null, cub) );
                    got_one = true;
                }
            } else if (job.reduce_keys.length > 0 && job.phase == REDUCE) {
                if (job.code.reducefn != null) {
                    //console.log('have reducefn');
                    //console.log(cub);
                    if (!(job.name in cub.active_jobs))
                        send_job(cub, job);
                    var key = job.reduce_keys.shift();
                    //console.log(key);
                    cub.tasks.push([job.name, [key, job.reduce_data[key]]]);
                    //console.log(cub.tasks)
                    job.active_count += 1;
                    cub.rpc.do_reduce(job.name, [[key, job.reduce_data[key]]], finish_task.bind(null, cub));
                    got_one = true;
                }
            }
        }
    }
    if (got_one) job_emitter.emit('schedule');
});

var send_job = function (cub, job) {
    console.log("sending job");
    //console.log(cub);
    //console.log(job);
    cub.active_jobs[job.name] = true;
    cub.rpc.new_job(job.name, job.code, voidfn);
}

var create_job = function (client, jobname, k_v_pairs, code, want_results, cb) {
    var job = {
        name: jobname + "-" + counter,
        map_data: k_v_pairs,
        reduce_data : {},
        reduce_keys : [],
        results: {},
        code : code,
        clients: [],
        active_count: 0,
        phase: MAP
        
    };
    counter += 1;
    if (want_results) job.clients.push(client);
    job_list.push(job.name);
    jobs[job.name] = job;
    job_emitter.emit('schedule');
    cb(true);
};

var new_cub = function (client, conn, cubname, cb) {
    var hostkey = conn.stream.remoteAddress + ":" + conn.stream.remotePort;
    console.log(hostkey);
    var cub = {
        name: cubname,
        hostkey: hostkey,
        rpc: client,
        tasks: [],
        active_jobs: {}
    };
    cubs[hostkey] = cub;
    job_emitter.emit('schedule');
    conn.on('end', function () { 
        for (var i in cubs[hostkey].tasks) {
            var task = cubs[hostkey].tasks[i];
            var job = jobs[task[0]];
            if (job.phase == MAP) {
                job.map_data.push(task[1]);
            }
            else if (job.phase == REDUCE) {
                job.reduce_keys.push(task[1][0]);
            }
            job.active_count -= 1;
        }
        delete cubs[hostkey]
        console.log("Lost cub");
        console.log(cubs);
        job_emitter.emit('schedule');
    });
}

master_exports = function (client, conn) {
    this.create_job = create_job.bind(null, client);
    this.new_cub = new_cub.bind(null, client, conn);
    this.reduce_result = reduce_result;
    this.map_result = map_result;
}

var httpServer = pumahttp.makeHttpServer();
httpServer.listen(8080);
DNode(master_exports).listen({
    protocol : 'socket.io',
    server : httpServer,
    transports : 'websocket xhr-multipart xhr-polling htmlfile'.split(/\s+/),
});
DNode(master_exports).listen(6060);

