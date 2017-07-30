#!/usr/bin/env node

const ls = require('./../lib/ls');
const pull = require('./../lib/pull');
const argv = require('minimist')(process.argv.slice(2));

function printUsage() {
    console.log('Usage: scuba <command> <stream-name> <optional args>');
}

if (argv._.length < 2) {
    console.log(`Error: missing arguments`);
    printUsage();
    process.exit(1);
}

if (argv._[0] === 'ls') ls(argv._[1]);
if (argv._[0] === 'pull') pull(argv);