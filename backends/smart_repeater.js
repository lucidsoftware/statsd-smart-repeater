// NEED config reload
// NEED stats

/*
 * Flush stats to a downstream statsd server.
 *
 * To enable this backend, include 'smart_repeater' in the backends
 * configuration array:
 *
 *   backends: ['smart_repeater']
 */

var util = require('util'),
    dgram = require('dgram'),
    net = require('net'),
    logger = require('../lib/logger');

var l;
var debug;

function SmartRepeater(startupTime, config, emitter){
	var self = this;

	this.config = config.smartRepeater || {};

	this.prefix = this.config.prefix || '';
	this.prefix = (this.prefix.length == 0) ? "" : (this.prefix + ".");
	this.batchSize = this.config.batchSize || 1024;

	this.hostinfo = [];
	for (var i = 0; i < this.config.hosts.length; i++) {
		var host = this.config.hosts[i];
		this.hostinfo.push({
			config: host,
			errors: 0,
			flushes: 0,
			bytesSent: 0,
			statsSent: 0,
			packetsSent: 0
		});
	}

	emitter.on('flush', function(time_stamp, metrics) { self.process(time_stamp, metrics); });
};

SmartRepeater.prototype.sampleRateToString = function(number) {
	var string = number.toFixed(3);
	var index = string.length - 1;

	if (string[index] == "0") {
		while (index >= 0 && (string[index] == "0" || string[index] == ".")) { index--; }
		if (index < 0) {
			string = '0';
		}
		else {
			string = string.substring(0, index + 1);
		}
	}

	return string;
};

SmartRepeater.prototype.reconstituteMessages = function(metrics) {
	var key, i;
	var outgoing = [];

	for (key in metrics.gauges) {
		outgoing.push(this.prefix + key + ":" + metrics.gauges[key] + "|g");
	}

	for (key in metrics.counters) {
		outgoing.push(this.prefix + key + ":" + metrics.counters[key] + "|c");
	}

	for (key in metrics.timers) {
		var values = metrics.timers[key];
		var sampleRate = values.length / metrics.timer_counters[key];
		var sampleRateString = (sampleRate >= 1) ? "" : ("|@" + this.sampleRateToString(sampleRate));
		for (i = 0; i < values.length; i++) {
			outgoing.push(this.prefix + key + ":" + values[i] + "|ms" + sampleRateString);
		}
	}

	for (key in metrics.sets) {
		var values = metrics.sets[key].values;
		for (i = 0; i < values.length; i++) {
			outgoing.push(this.prefix + key + ":" + values[i] + "|s");
		}
	}

	return outgoing;
};

SmartRepeater.prototype.sendToHost = function(host, data) {
	var self = this;
	var i;

	try {
		if (host.config.protocol == "udp4" || host.config.protocol == "udp6") {
			var sock = dgram.createSocket(host.config.protocol);
			for (i = 0; i < data.length; i++) {
				var single = data[i];
				var buffer = new Buffer(single);
				console.log("Sending", single, host.config);
				sock.send(buffer, 0, single.length, host.config.port, host.config.hostname, function(err, bytes) {
					if (err && debug) {
						l.log(err);
					}
				});
			}
		}
		else {
			var connection = net.createConnection(host.config.port, host.config.hostname);
			connection.addListener('error', function(connectionException) {
				if (debug) {
					l.log(connectionException);
				}
			});
			connection.on('connect', function() {
				for (i = 0; i < data.length; i++) {
					var single = data[i];
					this.write(single);
				}
				this.end();
			});
		}
	}
	catch(e) {
		if (debug) {
			l.log(e);
		}
	}
};

SmartRepeater.prototype.distribute = function(reconstituted) {
	var self = this;
	var i;

	var buffers = [];
	var buffer = [];
	var bufferLength = 0;

	for (i = 0; i < reconstituted.length; i++) {
		var line = reconstituted[i];
		var lineLength = line.length;

		if (bufferLength != 0 && (bufferLength + lineLength) > this.config.batchSize) {
			buffers.push(buffer);
			buffer = [];
			bufferLength = 0;
		}

		buffer.push(line);
		bufferLength += lineLength;
	}

	if (bufferLength > 0) {
		buffers.push(buffer);
	}

	var lines = [];
	for (i = 0; i < buffers.length; i++) {
		lines.push(buffers[i].join("\n"));
	}

	for (i = 0; i < this.hostinfo.length; i++) {
		var host = this.hostinfo[i];
		this.sendToHost(host, lines);
	}
};

SmartRepeater.prototype.process = function(time_stamp, metrics) {
	var self = this;

	var processStart = Date.now();
	var reconstituted = this.reconstituteMessages(metrics);
	this.distribute(reconstituted);
	var processEnd = Date.now();
};

exports.init = function(startupTime, config, events) {
	var instance = new SmartRepeater(startupTime, config, events);
	l = new logger.Logger(config.log || {});
	debug = config.debug;
	return true;
};
