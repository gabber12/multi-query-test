const async = require('async');
const elasticsearch = require('elasticsearch');
const text_analyzer = require('pelias-text-analyzer');

const client = new elasticsearch.Client({
  host: "internal-pelias-dev1-es2-838308460.us-east-1.elb.amazonaws.com:9200",
  //host: "internal-pelias-prodbuild-es2-2062091370.us-east-1.elb.amazonaws.com:9200"
  //host: 'localhost:9200',
  //log: 'trace'
});
const mapping = {
  city: 'locality',
  state: 'region'
};


function remapToOurLayers(parsed_query) {
  var new_mapping = { };

  Object.keys(parsed_query).forEach(function(key) {
    if (mapping.hasOwnProperty(key)) {
      new_mapping[mapping[key]] = parsed_query[key];
    }  else {
      new_mapping[key] = parsed_query[key]
    }
  });

  if (!new_mapping.name) {
    if (parsed_query.number && parsed_query.street) {
      new_mapping.name = `${parsed_query.number} ${parsed_query.name}`
    }
  }

  return new_mapping;
}


// a list of layers in descending order. earlier entries are searched first
const descendingHierarchy = [ 'country', 'region', 'locality', 'borough', 'neighbourhood' ];

/*
 * Generate an elasticsearch query to search for a record of a given name in a given layer.
 * Optionally, a parent element id can be specified. In that case
 * only documents with that parent (in any field) will be returned
 */
function generateQuery(layer, name, parent_id, parent_layer) {
  // create a base query with only the name match
  const query = {
    index: 'pelias',
    type: layer,
    body: {
      query: {
        bool: {
          must: [
            {
              match: {
                'name.default':  {
                  query: name,
                  analyzer: 'peliasQueryFullToken'
                }
              }
            }
          ]
        }
      }
    }
  };

  // if a parent id was specified, add a second condition
  // matching on any parent id field
  if (parent_id && parent_layer) {
    const field = `parent.${parent_layer}_id`;
    query.body.query.bool.must.push({
      term: {
        [field]: parent_id
      }
    });
  }

  // if layer is specified and is not address or venue, only search WOF
  // Geonames does not have a good hierarchy for use with these queries
  if (layer && layer !== 'address' && layer !== 'venue') {
    query.body.query.bool.must.push({
      term: {
        source: 'whosonfirst'
      }
    });
  }

  return query;
}

function printResult(resp) {
  if (resp.hits.hits.length === 0 ) {
    console.log('no hits');
    return;
  }

  const hit = resp.hits.hits[0];
  const layer = hit._source.layer;
  const name = hit._source.name.default;
  const id = hit._id;
  const time = resp.took;

  console.log(`[${layer}] ${id} ${name} (${time}ms)`);
}

function getLastParent(parentObjects) {
    // if there is already a parent, use that id in the query
    if (parentObjects.length > 0) {
      var fullParent = parentObjects[parentObjects.length - 1];
      return {
        id: fullParent._id,
        layer: fullParent._source.layer
      };
    } else {
      return {};
    }
}

function runDescendingQueries(query) {
  var parentObjects = [];
  async.eachSeries(descendingHierarchy, function queryLayer(layer, callback) {
    // skip if no query element for this layer
    if (query[layer] === undefined) {
      return callback();
    }

    const parent = getLastParent(parentObjects);

    client.search(generateQuery(layer, query[layer], parent.id, parent.layer), function(err, resp) {
      printResult(resp);
      parentObjects.push(resp.hits.hits[0]);
      callback();
    });
  }, function(err) {
    console.log('done querying parent hierarchy');

    const parent = getLastParent(parentObjects);

    client.search(generateQuery(undefined, query.name, parent.id, parent.layer), function(err, resp) {
      printResult(resp);
    });
  });
}

var input = process.argv.slice(2).join(' ');

console.log(`searching for ${input}`);
const parsed = text_analyzer.parse(input);
const remapped = remapToOurLayers(parsed);

runDescendingQueries(remapped);
