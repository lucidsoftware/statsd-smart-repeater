statsd-smart-repeater
=====================

A pluggable backend for [StatsD](https://github.com/etsy/statsd) that aggregates and sends metrics out to another StatsD server.
This repeater can be used to aggregate metrics locally before sending them to the master StatsD instance.
It can also be used to replicate StatsD metrics across multiple masters.

Installation and Configuration
------------------------------

 * Put smart_repeater.js into your StatsD backends directory
 * Add its config parameters to your StatsD config file (see exampleConfig.js)
 * Restart the StatsD daemon
