var argv = require('minimist')(process.argv.slice(2));
var redis = require('redis').createClient();

var workerName = (+new Date()) + '-' + process.pid;
var faultTimeCheck = 500; //ms
var generateMessages = false;
var generateMessagesSpeed = typeof argv.speed !== 'undefined' ? parseInt(argv.speed) : 500;
var workerStaleTime = 100;
var messageId = 0;
var logLevel = argv.level || 3; //3 for debbuging, 1 for benchmarks
var stats = {
	generated: 0,
	processed: 0,
	successes: 0,
	fails: 0
};

if(logLevel > 0) console.log('Hi, I am worker %s', workerName);

redis.on('error', function(err){
	throw new Error(err);
});

redis.on('ready', start);

function printStats() {
	if(logLevel > 0) {
		console.log('Worker %s processed %s messages, %s successes, %s fails. Generated %s messages.',
			workerName,
			stats.processed,
			stats.successes,
			stats.fails,
			stats.generated
		);
		setTimeout(printStats, 5000);
	}
}

function start() {
	if(argv.getErrors) {
		getAndPrintErrors();
		return;
	}
	
	setTimeout(printStats, 5000);
	pollToBeGenerator();
	worker();
}

function getAndPrintErrors() {
	redis.zrange('ZSET:ERRORS', 0, -1, onGet);
	
	function onGet(e, ids) {
		if(e) throw new Error(e);
		if(!ids || ids.length < 1) {
			console.log('There is no errors');
			process.exit(0);
		}
		var pipe = redis.multi();
		ids.forEach(function(id){
			pipe.get('KV:ERROR:MESSAGE:'+id, printMessage);
			pipe.zrem('ZSET:ERRORS', id);
			pipe.del('KV:ERROR:MESSAGE:'+id);
		});
		pipe.exec(function(e){
			if(e) throw new Error(e);
			process.exit(0);
		});
		
		function printMessage(e, jsonStr) {
			if(e) throw new Error(e);
			try {
				var json = JSON.parse(jsonStr);
				//console.log(json, jsonStr);
				console.log(Array(50).join('='));
				console.log('Message id %s\nError: %s\nMessage: %s', json.id, json.error, json.msg);
			} catch(e) {
				console.log(Array(50).join('='));
				console.log('Something wrong with JSON, `%s`', jsonStr);
			}
		}
	}
}

function pollToBeGenerator() {
	if(generateMessages) return;
	setTimeout(pollToBeGenerator, faultTimeCheck);
	redis.setnx('KV:GENERATORAPP', workerName, function(e, win){
		if(e) throw new Error(e);
		if(logLevel > 2) console.log('setnx return %s', win); 
		generateMessages = !!win; 
		if(win) {
			console.log('I am a generator!');
			return generate();
		}
	});
}

function generate() {
	if(!generateMessages) return;
	if(logLevel > 2) console.log('generate new message'); 
	setTimeout(generate, generateMessagesSpeed);
	stats.generated++;
	var pipe = redis.multi();
	pipe.set('KV:GENERATORAPP', workerName);
	pipe.pexpire('KV:GENERATORAPP', faultTimeCheck*3);
	//Don`t want use a redis.incr, just generating a uniq id.
	//Worker name should be uniq, messageId I need for sending more then 1 message at 1 ms.
	var msgId = workerName + '-' + (+new Date()) + '-' + (++messageId);
	var message = {
		someDummyData: +new Date()
	};
	pipe.set('KV:MESSAGE:'+msgId, JSON.stringify(message, null, '\t'));
	pipe.lpush('LIST:MESSAGES', msgId);
	pipe.exec(function(e){
		if(e) throw new Error(e);
	});
}

function worker() {
	redis.rpop('LIST:MESSAGES', function(e, msgId){
		if(e) throw new Error(e);
		if(!msgId) {
			//nothing in queue
			setTimeout(worker, workerStaleTime);
			return;
		}
		
		redis.get('KV:MESSAGE:'+msgId, processMessage);
		
		function processMessage(e, jsonStr){
			if(e) throw new Error(e);
			process.nextTick(worker);
			try {
				var json = JSON.parse(jsonStr);
				if(logLevel > 2) console.log('process message id %s', msgId, json); 
				eventHandler(jsonStr, onCallback);
			} catch(e) {
				console.log('something wrong with json in redis, json: %s', jsonStr);
			}
		}
		
		function onCallback(e, msg) {
			stats.processed++;
			if(!e) {
				stats.successes++;
				redis.del('KV:MESSAGE:'+msgId);
				return;
			}
			stats.fails++;
			var pipe = redis.multi();
			pipe.set('KV:ERROR:MESSAGE:'+msgId, JSON.stringify({
				id: msgId,
				error: e,
				msg: msg
			}, null, '\t'));
			pipe.zadd('ZSET:ERRORS', +new Date(), msgId);
			pipe.exec(function(e){
				if(e) throw new Error(e);
			});
		}
	});
}

function eventHandler(msg, callback){
	function onComplete(){
		var error = Math.random() > 0.85; callback(error, msg);
	}
	// processing takes time...
	setTimeout(onComplete, Math.floor(Math.random()*1000));
}
