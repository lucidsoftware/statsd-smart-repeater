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
			flushTime: 0,
			flushes: 0,
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

SmartRepeater.prototype.sendToHost = function(host, metrics) {
	var i;

	var starttime = Date.now();

	var data = this.splitStats([
		this.prefix + "statsd-smart-repeater.errors:" + host.errors + "|g",
		this.prefix + "statsd-smart-repeater.flushTime:" + host.flushTime + "|g",
		this.prefix + "statsd-smart-repeater.flushes:" + host.flushes + "|g",
	]).concat(metrics);

	try {
		if (host.config.protocol == "udp4" || host.config.protocol == "udp6") {
			var sock = dgram.createSocket(host.config.protocol);
			for (i = 0; i < data.length; i++) {
				var single = data[i];
				var buffer = new Buffer(single);
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
				host.errors++;
			});
			connection.on('connect', function() {
				for (i = 0; i < data.length; i++) {
					var single = data[i];
					if (i != 0) {
						this.write("\n");
					}
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
		host.errors++;
	}

	host.flushTime = (Date.now() - starttime);
	host.flushes++;
};

SmartRepeater.prototype.splitStats = function(stats) {
	var i;

	var buffers = [];
	var buffer = [];
	var bufferLength = 0;

	for (i = 0; i < stats.length; i++) {
		var line = stats[i];
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

	return lines;
}

SmartRepeater.prototype.distribute = function(reconstituted) {
	var i;

	var lines = this.splitStats(reconstituted);

	for (i = 0; i < this.hostinfo.length; i++) {
		var host = this.hostinfo[i];
		this.sendToHost(host, lines);
	}
};

SmartRepeater.prototype.process = function(time_stamp, metrics) {
	this.distribute(this.reconstituteMessages(metrics));
};

exports.init = function(startupTime, config, events) {
	var instance = new SmartRepeater(startupTime, config, events);
	l = new logger.Logger(config.log || {});
	debug = config.debug;
	return true;
};
