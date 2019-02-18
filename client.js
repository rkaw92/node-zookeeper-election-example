'use strict';

const uuidv4 = require('uuid/v4');
const processID = uuidv4();
const path = require('path').posix;

function log(msg, ...args) {
	console.log.apply(console, [ '[%s] ' + msg, processID, ...args ]);
}

function fail(error) {
	throw error;
}

function waitUntilLeader(client, myKey, leaderCallback) {
	client.getChildren('/election', function(error, children) {
		if (error) {
			return void fail(error);
		}
		children.sort();
		log('children: %j', children);
		if (!children || !children[0]) {
			return void fail(new Error('No children found under the election path - ZooKeeper may be possessed'));
		}
		if (children[0] === myKey) {
			leaderCallback();
		} else {
			let recursed = false;
			client.exists(path.join('/election', children[0]), function watcher(event) {
				if (event.getType() === zookeeper.Event.NODE_DELETED) {
					recursed = true;
					waitUntilLeader(client, myKey, leaderCallback);
				}
			}, function _existsCallback(error, stat) {
				if (error) {
					return void fail(error);
				}
				if (!stat) {
					recursed = true;
					waitUntilLeader(client, myKey, leaderCallback);
				}
			});
		}
	});
}

const zookeeper = require('node-zookeeper-client');
const client = zookeeper.createClient('localhost:2181,localhost:2182,localhost:2183');
client.on('connected', function() {
	log('connected to ZK');
	client.create('/election', null, function(error) {
		if (error && error.getCode() == zookeeper.Exception.NODE_EXISTS) {
			log('/election exists (this is normal)');
		} else if (error) {
			return void fail(error);
		} else {
			log('created /election');
		}
		// BUG: This does not check for previous znodes created by us. As a result,
		//  on reconnect, if our session is still valid (non-expired), we
		//  encounter an ephemeral node that:
		// 1. never vanishes because it's created by us
		// 2. we don't consider our own
		// This could be fixed by remembering the created znode per-session and
		//  skipping creation if we believe our session is still alive.
		// However, an issue arises if we disconnect during the znode creation
		//  and are not sure whether the creation succeeded or not. Since the
		//  creation of sequential znodes is non-idempotent, and session ID
		//  is maintained between reconnection attempts, we have 2 options:
		// A: Abandon the session if this creation operation fails (possibly HACF)
		// B: Try finding our own nodes first (by checking ephemeralOwner).
		client.create('/election/p_', null, undefined, zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, function(error, createdPath) {
			if (error) {
				return void fail(error);
			}
			log('my leader key is %s', createdPath);
			if (!leader) {
				waitUntilLeader(client, path.basename(createdPath), function() {
					log('I AM THE LEADER');
					leader = true;
				});
			}
		});
	});
});
client.on('disconnected', function() {
	log('disconnected from ZK - do not panic, we will try connecting again');
});
client.on('expired', function() {
	log('client expired - bail out!');
	process.exit(13);
});

client.connect();
