/*

Required Variables:

	smartRepeater:
		prefix:               stat prefix (prepended to all outgoing stats)
		batchSize:            max message size to send downstream
		hosts:                array of host data to repeat information to
			hostname:         hostname or ip address
			port:             TCP port
			protocol:         udp4, udp6, tcp4, tcp6

Optional Variables:

	smartRepeater:
		prefix:               stat prefix (prepended to all outgoing stats)
		checkExistingPrefix:  true to not prepend the prefix if it's already there
		batchSize:            max message size to send downstream

*/

{
	/* ---------------------------------- */
	/* insert original statsd config here */
	/* ---------------------------------- */

	smartRepeater: {
		prefix: '',
		checkExistingPrefix: false,
		batchSize: 1024,
		hosts: [
			{ hostname: 'localhost', port: 8125, protocol: 'tcp6' },
			{ hostname: '127.0.0.1', port: 8127, protocol: 'udp4' }
		]
	}
}
