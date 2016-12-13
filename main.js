#!/usr/bin/env node

const http = require('http');
const fetch = require('node-fetch');
const fs = require('fs');


const segmentSize = 100; // # experiments to retrieve per segment


// From the search result data, get the list of experiment accessions as an array of strings.
function getIdsFromData(data) {
    return data['@graph'].map(result => result['@id']);
}


// Given a search result, get the total number of experiments in the database
function getExperimentTotalFromResult(result) {
    const typeFacet = result.facets.find(facet => facet.field === 'type');
    const experimentTypeTerm = typeFacet.terms.find(term => term.key === 'Experiment');
    return experimentTypeTerm.doc_count;
}


function getExperiment(host, experimentId) {
    const url = `${host}${experimentId}`;
    return fetch(url, {
        method: 'GET',
        headers: {
            Accept: 'application/json',
        },
    }).then((response) => {
        // Convert response to JSON
        if (response.ok) {
            return response.text();
        }
        throw new Error('not ok');
    }).catch((e) => {
        console.log('OBJECT LOAD ERROR: %s', e);
    });
}


// Issue a GET request on ENCODE data and return a promise with the ENCODE search response.
// - start: starting search result index of data being requested. default 0.
// - count: Number of entries to retrieve. default is ENCODE system default. 'all' for all
//          entries.
function getSegment(host, start, count) {
    const url = `${host}/search/?type=Experiment${count ? `&limit=${count}` : ''}${start ? `&from=${start}` : ''}`;
    return fetch(url, {
        method: 'GET',
        headers: {
            Accept: 'application/json',
        },
    }).then((response) => {
        // Convert response to JSON
        if (response.ok) {
            return response.text();
        }
        throw new Error('not ok');
    }).then((body) => {
        // Convert JSON to Javascript object, then attach start index so we can sort the
        // segments later if needed
        try {
            const result = JSON.parse(body);
            result.startIndex = start;
            return Promise.resolve(result);
        } catch (error) {
            console.log('ERR: %s,%o', error, body);
        }
        return Promise.resolve();
    }).catch((e) => {
        console.log('OBJECT LOAD ERROR: %s', e);
    });
}


function getExperimentsIds(host) {
    // Send an initial GET request to search for segment of experiments, so we can get the
    // total number of experiments.
    return getSegment(host, 0, segmentSize).then((result) => {
        const totalExperiments = getExperimentTotalFromResult(result);

        // Display the total number of experiments.
        console.log('Total experiments: %s', totalExperiments);

        // Add this set of experiment @ids to the array of them we're collecting.
        let experimentIds = getIdsFromData(result);

        // Now get ready the experiment segment retrieval loop. We'll get a segment of
        // experiments and extract their @ids until we have all of them. We'll do this by first
        // making an array called `searchParms` of simple objects containing the starting index
        // and count for the segment.
        const searchParms = (() => {
            let start = 0;
            let experimentsLeft = totalExperiments - experimentIds.length;
            const parms = [];
            while (experimentsLeft > 0) {
                const currSegmentSize = experimentsLeft > segmentSize ? segmentSize : experimentsLeft;
                parms.push({ start: start, count: currSegmentSize });
                start += currSegmentSize;
                experimentsLeft = totalExperiments - start;
            }
            return parms;
        })();

        // Send out all our segment GET requests.
        return searchParms.reduce((promise, parm) =>
            promise.then(() =>
                // Send the GET request for one segment
                getSegment(host, parm.start, parm.count)
            ).then((segment) => {
                // Got one segment of experiments. Add it to our array of @ids in retrieval order for now.
                experimentIds = experimentIds.concat(getIdsFromData(segment));

                return experimentIds;
            }), Promise.resolve(experimentIds)
        );
    });
}


process.argv.shift()  // skip node.exe
process.argv.shift()  // skip name of js file
let host = process.argv[0] ? process.argv[0] : 'http://localhost:6543';
host = host[host.length - 1] === '/' ? host.slice(0, -1) : host;
console.log('Using host %s', host);


getExperimentsIds(host).then(experimentIds => {
    const experimentStats = [];
    let i = 0;

    return experimentIds.reduce((promise, experimentId) =>
        promise.then(() =>
            // With an experimentId, request the experiment itself.
            getExperiment(host, experimentId)
        ).then((experiment) => {
            let trimmedExperiment = experiment;

            // Convert to object and remove audits if they exist.
            const experimentObj = JSON.parse(experiment);
            if (experimentObj.audit) {
                delete experimentObj.audit;
                trimmedExperiment = JSON.stringify(experimentObj);
            }

            // Got the experiment; add its ID and size to our array of experiments
            experimentStats.push({ id: experimentId, size: trimmedExperiment.length });

            // Every 20 experiments, give some progress.
            i += 1;
            if (i % 20 === 0) {
                console.log('Got %s experiments', i);
            }

            // Return the updated array of statistics.
            return experimentStats;
        }), Promise.resolve(experimentIds)
    )
}).then((experimentStats) => {
    const sortedStats = experimentStats.sort((a, b) => b.size - a.size);
    let output = '';
    sortedStats.forEach(stat => {
        output = output.concat(`${stat.id}\t${stat.size}\n`);
    });
    fs.writeFile("./experimentsizes.tsv", output, (err) => {
        if (err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });
});
